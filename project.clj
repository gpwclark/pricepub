(defproject pricepub "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [byte-streams "0.2.4"]
                 [aleph "0.4.6"]
                 [digest "1.4.8"]
                 [gloss "0.2.6"]]
  :main pricepub.core
  :aot [pricepub.core]
  )
