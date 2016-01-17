(ns dataview.test.setup
  (:import [datomic.peer Connection])
  (:require [clojure.test :refer [deftest is]]
            [datomic.api :as d]))

(def db-uri "datomic:free://localhost:4334/dataview-test")

(defn create-db []
  (d/create-database db-uri))

(defn delete-db []
  (d/delete-database db-uri))

(defmacro with-conn
  [& body]
  `(let [~(symbol "conn") (d/connect db-uri)]
     (do
       ~@body)))
