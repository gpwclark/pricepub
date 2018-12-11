(ns pricepub.api
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

(defmulti publish
  (fn [impl] (:impl impl)))

(defmethod publish :pricepub [impl]
  (let [{con-info :con-info
         topic :topic
         payloads :payloads} impl]
    ;;(pub/send-messages con-info messages)
    (pub/on-topic-send-payloads con-info topic payloads)))

(defmethod publish :aleph [impl]
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

(defmulti subscribe
  (fn [impl] (:impl impl)))

(defmethod subscribe :default [impl]
  (let [con-info (:con-info impl)
        topics (:topic impl)]
    (sub/subscribe-to con-info topics)))
