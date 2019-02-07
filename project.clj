(defproject clone-rdd "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.clojure/data.json "0.2.6"]
                 [yieldbot/flambo "0.7.2"]
                 [org.apache.spark/spark-core_2.10 "1.6.3"]
                 [org.apache.spark/spark-streaming_2.10 "1.6.3"]
                 [org.apache.spark/spark-sql_2.10 "1.6.3"]
                 [org.apache.spark/spark-hive_2.10 "1.6.3"]
                 [com.databricks/spark-csv_2.10 "1.5.0"]
                 ]
  :aot :all
  :main ^:skip-aot clone-rdd.core
  :profiles {:provided
             {:dependencies
              [[org.apache.spark/spark-core_2.10 "1.6.3"]]}})
