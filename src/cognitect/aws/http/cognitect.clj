;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

(ns ^:skip-wiki cognitect.aws.http.cognitect
  (:require [clojure.tools.logging :as log]
            [cognitect.http-client :as impl]
            [cognitect.aws.http :as aws]))

(set! *warn-on-reflection* true)

(defn create
  []
  (let [c (impl/create {:follow-redirects false})]
    (reify aws/HttpClient
      (-submit [_ request channel]
        (log/info (str "Sending request to "
                       (name (:scheme request)) "://"
                       (:server-name request) (:uri request)))
        (impl/submit c request channel))
      (-stop [_]
        (impl/stop c)))))
