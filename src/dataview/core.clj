(ns dataview.core
  (:refer-clojure :exclude [name])
  (:require [datascript.core :as ds]
            [datomic.api :as d]
            [datomic-schema.schema :as s]))

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

(defprotocol IDerivedAttrs
  (derived-attrs [this]))

(defprotocol IBuild
  (build! [this conn]))

(defprotocol IData
  (get-data [this])
  (set-data! [this data])
  (update-data! [this f] [this k f]))

(defprotocol IQuery
  (query [this query*]
         [this query* where]))

(defprotocol IContainer
  (create-schemas [this]))

;;;; Data view validation

(defn- view-name-valid? [name]
  (keyword? name))

(defn- view-schema-valid? [schema]
  (map? schema))

(defn- view-data-valid? [query]
  true)

(defn- view-derived-attrs-valid? [attrs]
  (and (map? attrs)
       (every? (fn [[k v]] (keyword? k)) attrs)))

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
    (eval `(s/schema ~name (s/fields ~@fields)))))

(defn schema->joins [schema]
  (letfn [(field->join [res [k v]]
            (cond-> res
              (map? v) (conj {k (first (vals v))})))]
    (reduce field->join [] schema)))

(defn field-name->attr-name [ename fname]
  (keyword (clojure.core/name ename)
           (clojure.core/name fname)))

(defn schema->pull-query [name schema]
  (let [attrs      (into []
                         (comp (remove join?)
                            (map (fn [[fname fspec]]
                                   (field-name->attr-name name fname))))
                         schema)
        joins      (schema->joins schema)
        join-attrs (mapv (fn [m]
                           (let [[fname view] (first m)]
                             (hash-map (field-name->attr-name name fname)
                                       (cond-> view
                                         (not= view '...) pull-query))))
                         joins)]
    (into [] (concat [:db/id] attrs join-attrs))))

;;;; Data view definition

(defn derive-attr [view conn entity [fname fspec]]
  (let [aname (field-name->attr-name (name view) fname)
        aval  (d/q fspec (d/db conn) (:db/id entity))]
    (println "derive attr" aname aval)
    (assoc entity aname aval)))

(defn derive-attrs [view conn entity]
  (println "derive attrs" view entity)
  (reduce #(derive-attr view conn %1 %2)
          entity
          (derived-attrs view)))

(defrecord DataView [config db]
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

  IQuery
  (query [this query*]
    (query this query* []))

  (query [this query* where]
    (let [where' (into [] (concat ['[?x]] where))]
      (ds/q `[:find [(~'pull ~'?x ~'?query) ...]
                      :in ~'$ ~'?query
                      :where ~@where']
                    @db query*)))

  IDerivedAttrs
  (derived-attrs [this]
    (:derived-attrs config))

  IBuild
  (build! [this conn]
    (println "build!" this conn)
    (let [data (d/q (:data config)
                    (d/db conn)
                    (pull-query this))]
      (println "build! data:" data)
      (->> data
           (mapv #(derive-attrs this conn %))
           (ds/transact! db)))))

(defn dataview [{:keys [name schema data derived-attrs]}]
  [{:pre [(view-name-valid? name)
          (view-schema-valid? schema)
          (view-data-valid? data)
          (view-derived-attrs-valid? derived-attrs)]}]
  (DataView. {:name name
              :schema schema
              :datomic-schema (schema->datomic-schema name schema)
              :data data
              :derived-attrs derived-attrs}
             (ds/create-conn)))

;;;; Data view container

(defrecord DataViewContainer [config]
  IContainer
  (create-schemas [this]
    (let [schemas (mapv datomic-schema (:views config))]
      (when (:create-schemas config)
        ((:create-schemas config) schemas)))))

(defn container
  [{:keys [views create-schemas notify]}]
  {:pre [(every? #(instance? DataView %) views)]}
  (let [container* (DataViewContainer.
                    {:views views
                     :create-schemas create-schemas
                     :notify notify})]
    (dataview.core/create-schemas container*)))
