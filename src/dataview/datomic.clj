(ns dataview.datomic
  (:require [datomic.api :as d]
            [datomic-schema.schema :as s]
            [dataview.core :as dv]))

;;;; Datomic schema generation

(defn- kv->field [res [k v]]
  (cond
    (vector? v) (conj res (into [] (concat [k] v)))
    (map? v)    (kv->field res [k (ffirst v)])
    :else       (conj res [k v])))

(defn- attrs->fields [schema]
  (reduce kv->field [] schema))

(defn- datomic-schema [{:keys [name attrs]}]
  (let [fields (attrs->fields attrs)]
    (eval `(s/schema ~name (s/fields ~@fields)))))

(defn install-schemas
  [conn schemas]
  (let [dschemas (mapv datomic-schema schemas)]
    (->> (concat (s/generate-parts [(s/part "app")])
                 (s/generate-schema dschemas))
         (d/transact conn)
         (deref))))

;;;; Pull (attr) query generation

(defn- attr-name [schema-name short-name]
  (keyword (name schema-name) (name short-name)))

(defn- join? [[short-name spec]]
  (and (map? spec) (= 1 (count spec))))

(defn- gather-attr-names [schema]
  {:pre [(map? schema)
         (contains? schema :attrs)
         (map? (:attrs schema))]}
  (into []
        (comp (remove join?)
              (map (fn [[name attr-spec]]
                     (attr-name (:name schema) name))))
        (:attrs schema)))

(defn- gather-joins [schema]
  {:pre [(map? schema)
         (contains? schema :name)
         (contains? schema :attrs)
         (map? (:attrs schema))]}
  (letfn [(join-attr [schema-name short-name spec]
            (let [[spec target] (first spec)]
              (hash-map (attr-name schema-name short-name)
                        (cond-> target
                          (not= target '...) dv/query-attrs))))
          (gather-join [res [short-name spec]]
            (cond-> res
              (map? spec) (conj (join-attr (:name schema)
                                           short-name spec))))]
    (reduce gather-join [] (:attrs schema))))

(defn query-attrs
  [view]
  (let [schema     (dv/schema view)
        attr-names (gather-attr-names schema)
        joins      (gather-joins schema)
        ret (into [] (concat [:db/id] attr-names joins))]
    ret))

;;;; Query execution

(defn query
  [{:keys [conn]} query {:keys [inputs extra]}]
  {:pre [(or (vector? query)
             (map? query))
         (or (nil? inputs)
             (vector? inputs))]}
  (apply (partial d/q
                  (concat query extra)
                  (d/db conn))
         inputs))

;;;; Overview implementation for Datomic

(defn merge-attr
  [schema entity name value]
  (assoc entity (attr-name (:name schema) name) value))

(defn overview [config]
  {:pre [(map? config)]}
  (dv/overview
   (merge {:entity-id :db/id
           :install-schemas install-schemas
           :query-attrs query-attrs
           :query query
           :transform dv/transform-specter
           :merge-attr merge-attr}
          config)))
