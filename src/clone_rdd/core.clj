(ns clone-rdd.core
  (:require [clone-rdd.utils :as util]
            [flambo.sql :as sql]
            [flambo.api :as api])
  (:gen-class))

(defn process-rdd[trx]
  (merge trx {"clone" (odd? (get trx "year"))}))

(defn perform-calcs[record]
  (if (true? (get record "clone"))
    (do
      (let [my-year (rand-int 20)]
        (println "\nmy-year:" my-year)
        (let [out-row (merge record {"year" my-year})]
          (println "\noutrow " out-row)
          out-row)))))

;; processing json in the following format, tp clojure map
(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (util/build-spark-local-context "welcome")
  (let [df            (sql/json-file util/sql-context "resources/cars.json")
        parent-rdd    (util/get-cached-cars-rdd df)
        final-rdd     (api/map parent-rdd
                               (api/fn [trx]
                                 (process-rdd trx)))
        final-one-rdd (->
                       (api/map final-rdd
                                (api/fn [trx]
                                  (perform-calcs trx)))
                       (api/filter (api/fn [trx]
                                     (not (nil? trx)))))]

    (sql/print-schema df)
    (println "parent " (.collect final-one-rdd) "\n" (.collect (.union final-rdd final-one-rdd)) "\n ")))

(-main)