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

;;;; Simple properties / IDataView interface functions

(defspec dataviews-satisfy-idataview
  (prop/for-all [schema-names (gen/vector gen/symbol)]
    (let [specs    (mapv (fn [schema-name]
                           {:schema {:name schema-name}})
                         schema-names)
          overview (dv/overview {:views specs})
          views    (mapv #(dv/get-view overview %) schema-names)]
      (and (is (every? #(not (nil? %)) views))
           (is (every? #(satisfies? dv/IDataView %) views))
           (is (= (into #{} schema-names)
                  (into #{} (map dv/get-name) views)))
           (is (= (into #{} (map :schema) specs)
                  (into #{} (map dv/schema) views)))))))

(defspec dataviews-have-the-same-name-as-their-schemas
  (prop/for-all [schema-name gen/symbol]
    (let [spec     {:schema {:name schema-name}}
          overview (dv/overview {:views [spec]})
          view     (dv/get-view overview schema-name)]
      (and (is (not (nil? view)))
           (is (= schema-name (dv/get-name view)))))))

(defspec dataviews-remember-their-schema
  (prop/for-all [schema-name gen/symbol]
    (let [spec     {:schema {:name schema-name}}
          overview (dv/overview {:views [spec]})
          view     (dv/get-view overview schema-name)]
      (and (is (not (nil? view)))
           (is (= (:schema spec) (dv/schema view)))))))

;;;; Simple data views for regular collections

(defspec vectors-can-be-used-as-dbs 10
  (prop/for-all [values (gen/vector gen/any)]
    (let [item     {:schema {:name 'item}
                    :sources [{:type :main}]}
          overview (dv/overview
                    {:views [item]
                     :conn values
                     :query (fn [{:keys [conn]} _ _] conn)})
          items    (dv/get-view overview 'item)]
      (is (= values (dv/query items nil))))))

(defspec queries-can-be-used-to-select-keys 10
  (prop/for-all [values (gen/vector (gen/map
                                     gen/simple-type
                                     gen/simple-type
                                     {:num-elements 200})
                                    10)]
    (let [item        {:schema {:name 'item}
                       :sources [{:type :main}]}
          overview    (dv/overview
                       {:views [item]
                        :conn values
                        :query (fn [{:keys [conn]} _ {:keys [inputs]}]
                                 (let [keys (first inputs)]
                                   (mapv #(select-keys % keys) conn)))})
          items       (dv/get-view overview 'item)
          common-keys (reduce clojure.set/intersection
                              (into #{} (keys (first values)))
                              (map (comp (partial into #{}) keys)
                                   (rest values)))]
      (is (= (into #{} (map #(select-keys % common-keys)) values)
             (into #{} (dv/query items (into [] common-keys))))))))
