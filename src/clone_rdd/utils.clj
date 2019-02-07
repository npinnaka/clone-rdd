(ns clone-rdd.utils
  (:require [flambo.api :as api]
            [flambo.conf :as conf]
            [flambo.sql :as sql])
  (:import [org.apache.spark.sql.types DataTypes]
           [org.apache.spark.sql Column
            RowFactory
            SaveMode]
           [scala.collection JavaConversions])
  (:gen-class))

(defn stock-dividends-schema []
  [["amount" DataTypes/DoubleType]
   ["date" DataTypes/StringType]])

(def stock-dividends-schema-vec
  (mapv first (stock-dividends-schema)))

(defn stock-schema []
  [["name" DataTypes/StringType]
   ["price" DataTypes/DoubleType]
   ["quantity" DataTypes/LongType]
   ["stock_dividends" (stock-dividends-schema)]])

(def stock-schema-vec
  (mapv first (stock-schema)))

(def parent-schema
  [["id" DataTypes/LongType]
   ["stocks" (stock-schema)]])

(def parent-schema-vec
  (mapv first parent-schema))

(api/defsparkfn build-map-from-rdd [rdd]
  (zipmap parent-schema-vec rdd))

(defn get-cached-rdd[df]
  (->
   df
   (.toJavaRDD)
   (api/map sql/row->vec)
   (api/map build-map-from-rdd)
   (api/cache)))

(api/defsparkfn build-map-from-cars-rdd [rdd]
  (zipmap ["make" "model" "year"] rdd))

(defn get-cached-cars-rdd[df]
  (->
   df
   (.toJavaRDD)
   (api/map sql/row->vec)
   (api/map build-map-from-cars-rdd)
   (api/cache)))

(defn build-spark-context[app-name]
  (defonce spark-context (api/spark-context (conf/spark-conf)))
  (defonce sql-context
    (sql/sql-context spark-context)))

(defn build-spark-local-context [app-name]
  (defonce spark-context (api/spark-context "local[*]" app-name))
  (defonce sql-context
    (sql/sql-context spark-context)))

(defn build-columns
  "prepare a column array"
  [& mycols]
  (into-array Column (map (fn [x] (Column. x)) mycols)))

(defn str-arry
  "prepare a string array"
  [& mycols]
  (into-array String mycols))

(defn save-file-with-partition[df partition-columns file-name]
  (->
   df
   (.write)
   (.mode SaveMode/Append)
   ;(.partitionBy (into-array partition-columns))
   (.save file-name)))

(defn create-structure
  [vec-map]
  (DataTypes/createStructType
   (map
    (fn map-field
      [[k v]]
      (DataTypes/createStructField (name k) v true))
    vec-map)))

(defn get-collection[scala-mutable-collection]
  (JavaConversions/asJavaCollection scala-mutable-collection))