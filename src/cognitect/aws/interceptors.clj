;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

(ns ^:skip-wiki cognitect.aws.interceptors
  "Impl, don't call directly."
  (:require [clojure.string :as str]
            [cognitect.aws.service :as service]
            [cognitect.aws.util :as util]))

(set! *warn-on-reflection* true)

(defmulti modify-http-request (fn [service _op-map _http-request]
                                (service/service-name service)))

(defmethod modify-http-request :default [_service _op-map http-request] http-request)

(def md5-blacklist
  "Set of ops that should not get the Content-MD5 header.

  See https://github.com/aws/aws-sdk-java-v2/blob/master/services/s3/src/main/java/software/amazon/awssdk/services/s3/internal/handlers/AddContentMd5HeaderInterceptor.java "
  #{:PutObject :UploadPart})

; See https://github.com/aws/aws-sdk-js/blob/4e678fbe5f8f3659391842675c6a59078c4b05f5/lib/services/s3util.js#L273
(def re-domain #"^[A-Za-z0-9][A-Za-z0-9\.\-]{1,61}[A-Za-z0-9]$")

; See https://github.com/aws/aws-sdk-js/blob/4e678fbe5f8f3659391842675c6a59078c4b05f5/lib/services/s3.js#L514-L524
(defn virtual-host-compatible-bucket-name? [s]
  (and (not (str/includes? s "."))
    (boolean (re-matches re-domain s))))

(defmethod modify-http-request "s3" [service op-map http-request]
  (let [{:keys [Bucket]} (:request op-map)
        {:keys [headers server-name uri]} http-request
        localstack? (= "localhost" server-name)
        vhost (str Bucket "." (when localstack? "s3.") server-name)
        http-request (if (and (some-> Bucket virtual-host-compatible-bucket-name?)
                              (str/starts-with? uri (str "/" Bucket "/")))
                       (assoc http-request
                              :headers (assoc headers "host" vhost)
                              :server-name vhost
                              :uri (subs uri (inc (count Bucket))))
                       http-request)]
    (if (and (= "md5" (get-in service [:metadata :checksumFormat]))
             (not (md5-blacklist (:op op-map)))
             (:body http-request))
      (update http-request :headers assoc "Content-MD5" (-> http-request :body util/md5 util/base64-encode))
      http-request)))

(defmethod modify-http-request "apigatewaymanagementapi" [_service op-map http-request]
  (if (= :PostToConnection (:op op-map))
    (update http-request :uri str (-> op-map :request :ConnectionId))
    http-request))

;; See https://github.com/aws/aws-sdk-java-v2/blob/985ec92c0dfac868b33791fe4623296c68e2feab/services/glacier/src/main/java/software/amazon/awssdk/services/glacier/internal/GlacierExecutionInterceptor.java#L40
(defmethod modify-http-request "glacier" [service _op-map http-request]
  (assoc-in http-request
            [:headers "x-amz-glacier-version"]
            (get-in service [:metadata :apiVersion])))