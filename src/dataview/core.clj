(ns dataview.core
  (:refer-clojure :exclude [name])
  (:require [clojure.core.async :refer [go]]
            [clojure.test :refer [function?]]
            [com.rpl.specter :as specter]))

;;;; Transformations

(defn transform-specter [tspec raw-data]
  (let [seq-data (if (coll? raw-data) raw-data [raw-data])
        tform    (into [] (conj tspec seq-data))
        res      (apply specter/transform tform)]
    (cond-> res
      (not (coll? raw-data)) first)))

;;;; Data view protocol

(defprotocol IDataView
  (get-name [this])
  (schema [this])
  (sources [this])
  (query-attrs [this])
  (query [this q]
         [this q extra]))

;;; Source queries

(defn query-main
  [{:keys [conn query-attrs query view]} source q extra]
  (let [attrs (query-attrs view)]
    (query conn view
           (:query source)
           {:inputs [attrs] :extra extra})))

(defn query-derived-attr
  [{:keys [data conn entity-id merge-attr
           query transform view]} source]
  (mapv (fn [entity]
          (let [id      (entity-id entity)
                raw     (query conn view
                               (:query source)
                               {:inputs [id]})
                tformed (cond->> raw
                          (:transform source)
                          (transform (:transform source)))]
            (merge-attr (schema view) entity
                        (:name source) tformed)))
        data))

(defmulti query-sources (fn [_ _ _ _ [type _]] type))

(defmethod query-sources :main
  [view q extra data [type sources]]
  (let [config     (:config view)
        merge-main (:merge-main config)]
    (reduce (fn [data source]
              (let [env   (merge config {:view view :data data})
                    sdata (query-main env source q extra)]
                (merge-main data sdata)))
            data
            sources)))

(defmethod query-sources :derived-attr
  [view _ _ ret [type sources]]
  (let [config (:config view)]
    (reduce (fn [ret source]
              (let [env (merge config {:view view :data ret})]
                (query-derived-attr env source)))
            ret sources)))

(defmethod query-sources :default
  [_ _ _ _ [type _]]
  (throw (Exception. (str "Unknown source type: " type))))

(defn compare-source-types [t1 t2]
  (let [order [:main :derived-attr]]
    (compare (.indexOf order t1) (.indexOf order t2))))

;;;; Data view implementation

(defrecord DataView [config]
  IDataView
  (get-name [this]
    (:name (schema this)))

  (schema [this]
    (:schema config))

  (sources [this]
    (:sources config))

  (query-attrs [this]
    ((:query-attrs config) this))

  (query [this q]
    (query this q nil))

  (query [this q extra]
    (let [sources (sources this)
          sorted  (sort-by :type compare-source-types sources)
          grouped (group-by :type sorted)]
      (reduce #(query-sources this q extra %1 %2)
              nil
              grouped))))

(defn dataview
  [{:keys [schema sources] :as config}]
  {:pre [(and (map? schema) (contains? schema :name))]}
  (DataView. config))

;;;; Overview protocol

(defprotocol IOverview
  (get-view [this name]))

;;;; Overview implementation

(defn- install-schemas! [overview]
  (let [config  (:config overview)
        conn    (:conn config)
        views   (:views config)
        schemas (mapv schema views)]
    ((:install-schemas config) conn schemas)))

(defrecord Overview [config]
  IOverview
  (get-view [this name]
    (let [views (:views config)]
      (first (filter #(= name (get-name %)) views)))))

(defn default-query-attrs [view]
  [])

(defn default-query
  [conn view query {:keys [inputs extra]}]
  conn)

(defn default-transform [tspec raw-value]
  (tspec raw-value))

(defn default-merge-main [mains main]
  (into [] (concat mains main)))

(defn default-merge-attr [schema entity name value]
  (assoc entity name value))

(defn overview
  [{:keys [views
           conn install-schemas
           entity-id
           query-attrs query
           transform
           merge-main merge-attr]
    :or {views           []
         conn            nil
         install-schemas #()
         entity-id       identity
         query-attrs     default-query-attrs
         query           default-query
         transform       default-transform
         merge-main      default-merge-main
         merge-attr      default-merge-attr}
    :as config}]
  {:pre [(map? config)]}
  (let [config'    {:views views
                    :conn conn
                    :install-schemas install-schemas
                    :entity-id entity-id
                    :query-attrs query-attrs
                    :query query
                    :transform transform
                    :merge-main merge-main
                    :merge-attr merge-attr}
        views'     (for [view-spec views]
                     (dataview (merge config' view-spec)))
        overview' (Overview. (assoc config' :views views'))]
    (install-schemas! overview')
    overview'))
