(ns pricepub.publish
  (:require [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.java.io :refer [writer reader]])
  (:import (java.net Socket)
           (java.io BufferedWriter)))

(defn write-to-buffer
  [output-stream string]
  (.write output-stream string)
  (.flush output-stream))

(defn write-to
  [socket message]
  (write-to-buffer (writer socket) message))

(defn send-all
  [sock messages]
  (loop [messages messages]
    (do
      (write-to sock (first messages))
      (recur (rest messages)))))

(defn connect
  [con-info]
  (let [host (:host con-info)
        port (:port con-info)]
    (Socket. host  port)))

(defn send-messages
  [con-info messages]
  (let [sock (connect con-info)]
    (do
      (send-all sock messages)
      (.close sock))))
