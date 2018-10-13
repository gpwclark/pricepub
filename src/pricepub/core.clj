(ns pricepub.core
  (:gen-class)
  (:require [clojure.data.json :as json]
            [aleph.tcp :as tcp]
            [clojure.edn :as edn]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [digest :as digest])
  (:gen-class))

(def con-info {:host "127.0.0.1" :port 7878})

;;sample data to play with
(def sample-payload-map {:topic "TOPIC" :payload_size 13 :checksum "shalskdjlkjsl"})
(def sample-payload "PAYLOAD_BYTES")
(def sample-message {:header (json/write-str sample-payload-map) :payload sample-payload})

(defn get-message-otw-format
  "[map]
  A map representing a message with :header and :payload keys, will be
  turned into a string of the header followed by the payload."
  [message]
  (apply str (:header message) (:payload message)))

(defn get-n-messages-in-otw-format
  "[message n]
  takes a map representing the message format and creates a list of size n
  where each element is a string that can be sent otw on the tcp client.
  Mainly used in testing with the sample message."
  [message n]
  (take n (repeat (get-message-otw-format message))))

(defn send-socket
  "[con-info messages]
  takes connection info and creates tcp connection to server and sends
  the list of messages (which are strings) to the server in one go."
  [con-info messages]
  (let [con (aleph.tcp/client con-info)
        fail (fn [x] (println "Failed to send messages:  " x))
        fire (fn [x] (do (s/put-all! x messages) (identity x)))]
    (-> con
        (d/chain fire)
        (deref)
        (s/close!)
        (d/catch Exception fail))))

(defn make-message
  "[topic payload]
  takes a topic and a payload string and turns it into a map representing
  the message format."
  [topic payload]
  (let [payload-bytes (.getBytes payload)
        size (count payload-bytes)
        checksum (digest/sha-1 payload-bytes)
        payload-map (-> '{}
                      (assoc :topic topic)
                      (assoc :payload_size size)
                      (assoc :checksum checksum))]
        {:header (json/write-str payload-map) :payload payload}))

(defn create-messages
  "[topic payloads]
  takes a topic and a list of payloads (both are all strings)
  and turns it into a list of messages in over the wire format"
  [topic payloads]
  (let [make-message (partial make-message topic)
        messages (map make-message payloads)
        messages-otw (map get-message-otw-format messages)]
  ;;NOTE: messages are returned in order because (map) is used twice.
  messages-otw))

(defn on-topic-send-payloads
  "[topic & payloads]
  Currently (problematically might I add) a string, topic, and a set of payload strings.
  All payloads will be sent on the given topic to the server."
  [topic payloads]
  (let [messages (create-messages topic payloads)]
    (send-socket con-info messages)))

(defn -main [topic & payloads]
  (on-topic-send-payloads topic payloads))

;; sample arguments
;;(on-topic-send-payloads "TOPIC" (list "I publish" "I batch" "but I do it" "sequentially"))
