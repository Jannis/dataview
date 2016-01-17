(ns dataview.core
  (:refer-clojure :exclude [name])
  (:require [datomic.api :as d]
            [datomic-schema.schema :as ds]))

;;;; Utilities

(defn drop-keys [m ks]
  (into {} (remove (fn [[k v]] (some #{k} ks))) m))

;;;; Protocols

(defprotocol IName
  (name [this]))

(defprotocol ISchema
  (schema [this])
  (datomic-schema [this]))

(defprotocol IPullQuery
  (pull-query [this]))

(defprotocol IBuild
  (build! [this conn]))

(defprotocol IData
  (get-data [this])
  (set-data! [this data])
  (update-data! [this f] [this k f]))

(defprotocol IQuery
  (query [this query & clauses]))

(defprotocol IContainer
  (create-schemas [this]))

;;;; Data view validation

(defn- view-name-valid? [name]
  (keyword? name))

(defn- view-schema-valid? [schema]
  (map? schema))

(defn- view-build-valid? [query]
  true)

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

(defrecord DataView [config data]
  IName
  (name [this]
    (symbol (clojure.core/name (:name (:config this)))))

  ISchema
  (schema [this]
    (:schema config))

  (datomic-schema [this]
    (:datomic-schema config))

  IPullQuery
  (pull-query [this]
    (schema->pull-query (name this) (schema this)))

  IData
  (get-data [this]
    @data)

  (set-data! [this new-data]
    (reset! data new-data))

  (update-data! [this f]
    (swap! data update (f data)))

  (update-data! [this k f]
    (swap! data update k f))

  IBuild
  (build! [this conn]
    (let [build-query (:build config)
          data (d/q build-query (d/db conn) (pull-query this))]
      (set-data! this data))))

(defn dataview [{:keys [name schema build]}]
  [{:pre [(view-name-valid? name)
          (view-schema-valid? schema)
          (view-build-valid? build)]}]
  (DataView. {:name name
              :schema schema
              :datomic-schema (schema->datomic-schema name schema)
              :build build}
             (atom #{})))

;;;; Data view container

(defrecord DataViewContainer [config]
  IContainer
  (create-schemas [this]
    (let [schemas (mapv datomic-schema (:views config))]
      (when (:create-schemas config)
        ((:create-schemas config) schemas)))))

(defn container
  [{:keys [views create-schemas notify]}]
  (let [container* (DataViewContainer.
                    {:views views
                     :create-schemas create-schemas
                     :notify notify})]
    (dataview.core/create-schemas container*)))
