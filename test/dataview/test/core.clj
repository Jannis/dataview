(ns dataview.test.core
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.rpl.specter :as specter :refer :all]
            [datomic.api :as d]
            [datomic-schema.schema :as ds]
            [dataview.core :as dv]
            [dataview.datomic :as dv-datomic]
            [dataview.test.setup :as setup :refer [create-db
                                                   delete-db
                                                   with-conn
                                                   connect-to-db]]))

;;;; Datomic setup

(defn datomic-fixture [f]
  (delete-db)
  (create-db)
  (f)
  (delete-db))

(use-fixtures :each datomic-fixture)

;;;; Specter transformations

(defspec transform-specter-works-on-vectors
  (prop/for-all [numbers (gen/vector gen/nat)]
    (is (= (mapv inc numbers)
           (dv/transform-specter [[ALL] inc] numbers)))))

(defspec transform-specter-works-on-maps
  (prop/for-all [numbers (gen/map gen/string gen/nat)]
    (is (= (zipmap (keys numbers) (map dec (vals numbers)))
           (dv/transform-specter [[ALL LAST] dec] numbers)))))

(defspec transform-specter-works-on-scalars
  (prop/for-all [val gen/simple-type]
    (is (= [:x val]
           (dv/transform-specter [[ALL] #(conj [:x] %)] val)))))
