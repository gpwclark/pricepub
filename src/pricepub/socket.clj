(ns pricepub.socket
  (:require [manifold.stream :as s]
            [manifold.deferred :as d]
            [aleph.tcp :as tcp]
            [pricepub.publish :as pub]
            [pricepub.subscribe :as sub])
  (:import (java.net InetSocketAddress)
           (java.nio.channels SocketChannel)))

(comment (defn partially-apply-sock
   [con-info sock-fn]
   (let [socket-addr (InetSocketAddress. (:host con-info) (:port con-info))]
     (with-open [socket-chan (SocketChannel/open socket-addr)]
       (partial sock-fn socket-chan)))))

(defmulti put
  (fn [impl] (:impl impl)))

(defmethod put :pricepub [impl]
  (let [con-info (:con-info impl)
        messages (:messages impl)]
    (pub/send-messages con-info messages)))

(defmethod put :aleph [impl]
  (let [con-info (:con-info impl)
        messages (:messages impl)
        con (tcp/client con-info)
        fail (fn [x] (println "Failed to send messages:  " x))
        fire (fn [x] (do (s/put-all! x messages) (identity x)))]
    (-> con
        (d/chain fire)
        (deref)
        (s/close!)
        (d/catch Exception fail))))

(defmulti get-topics
  (fn [impl] (:impl impl)))

(defmethod get-topics :default [impl]
  (let [con-info (:con-info impl)
        topic (:topic impl)]
    (sub/subscribe-to con-info topic)))
