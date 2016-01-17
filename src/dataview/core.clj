(ns dataview.core
  (:refer-clojure :exclude [name])
  (:require [datomic-schema.schema :as ds]))

;;;; Protocols

(defprotocol IName
  (name [this]))

(defprotocol ISchema
  (schema [this]))

(defprotocol IPullQuery
  (pull-query [this]))

;;;; Data view validation

(defn- view-name-valid? [name]
  (keyword? name))

(defn- view-schema-valid? [schema]
  (map? schema))

(defn schema? [x]
  (view-schema-valid? x))

(defn join? [[k v]]
  (and (map? v) (= 1 (count v))))

;;;; Schema parsing and translation

(defn kv->field [res [k v]]
  (cond
    (vector? v) (conj res (into [] (concat [k] v)))
    (map? v)    (kv->field res [k (ffirst v)])
    :else       (conj res [k v])))

(defn schema->fields [schema]
  (reduce kv->field [] schema))

(defn schema->datomic-schema [name schema]
  (let [fields (schema->fields schema)]
    (eval `(ds/schema ~name (ds/fields ~@fields)))))

(defn schema->joins [schema]
  (letfn [(field->join [res [k v]]
            (cond-> res
              (map? v) (conj {k (first (vals v))})))]
    (reduce field->join [] schema)))

(defn field->attr [ename [pname _ :as field]]
  (keyword (clojure.core/name ename) (clojure.core/name pname)))

(defn schema->pull-query [name schema]
  (let [attrs      (into []
                         (comp (remove join?)
                            (map #(field->attr name %)))
                         schema)
        joins      (schema->joins schema)
        join-attrs (mapv (fn [m]
                           (let [[pname view :as join] (first m)
                                 view-attrs            (if (= view '...)
                                                         '...
                                                         (pull-query view))
                                 join-with-attrs       [pname view-attrs]]
                             (hash-map (field->attr name join-with-attrs)
                                       view-attrs)))
                         joins)]
    (into [] (concat [:db/id] attrs join-attrs))))

;;;; Data view definition

(defmacro defdataview
  "Defines a new data view with the given name"
  [sym & {:keys [name schema]}]
  {:pre [(view-name-valid? name)
         (view-schema-valid? schema)]}
  (let [schema* (schema->datomic-schema name schema)]
    `(def ~(symbol sym)
       (reify
         IName
         (name [this]
           ~name)

         ISchema
         (schema [this]
           ~schema*)

         IPullQuery
         (pull-query [this]
           (schema->pull-query ~name ~schema))))))
