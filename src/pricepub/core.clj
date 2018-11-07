(ns pricepub.core
  (:gen-class)
  (:require [clojure.data.json :as json]
            [aleph.tcp :as tcp]
            [pricepub.api :as api]))

(def pub-con-info {:host "127.0.0.1" :port 7878})
(def sub-con-info {:host "127.0.0.1" :port 8787})

;;sample data to play with
;;(def sample-payload-map {:topic "TOPIC" :payload_size 13 :checksum "shalskdjlkjsl"})
;;(def sample-payload "PAYLOAD_BYTES")
;;(def sample-message {:header (json/write-str sample-payload-map) :payload sample-payload})

(def publish "pub")
(def subscribe "sub")

(defn error-incorrect-arg
  [arg]
  (throw (RuntimeException.
          (apply str "Valid first arguments are " publish " and "
                 subscribe "." "you provided: " arg "."))))

(defn -main
  [behavior topic & payloads]
  (cond
    (.equalsIgnoreCase behavior publish)
    (api/put
     {:impl :pricepub :con-info pub-con-info
      :topic topic :payloads payloads})
      ;;(api/put {:impl :pricepub :con-info con-info :messages messages}))
    (.equalsIgnoreCase behavior subscribe)
      (api/get-topics
      {:con-info sub-con-info
        :topic "{\"topics\":[\"TOPIC\"]}"})
        :else (error-incorrect-arg behavior)))

;; sample calls.
;;(api/put pub-con-info "TOPIC" (list "aphid" "beachball" "curmudgeon" "dillweed"))
;;(api/get-topics sub-con-info "{\"topics\":[\"TOPIC\"]}")
