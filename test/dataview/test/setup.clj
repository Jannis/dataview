(ns dataview.test.setup
  (:import [datomic.peer Connection])
  (:require [clojure.test :refer [deftest is]]
            [datomic.api :as d]))

(def db-uri "datomic:free://localhost:4334/dataview-test")

(defmacro with-conn
  [& body]
  `(let [~(symbol "_") (d/create-database db-uri)
         ~(symbol "conn") (d/connect db-uri)]
     (try
       (do
         ~@body)
       (finally
         (d/delete-database db-uri)))))

(deftest with-conn-works
  (with-conn
    (is (instance? Connection conn))))
