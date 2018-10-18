(ns pricepub.socket
  (:require [manifold.stream :as s]
            [manifold.deferred :as d]
            [aleph.tcp :as tcp]
            [pricepub.publish :as pub]))

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
