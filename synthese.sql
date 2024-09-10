-- synthese.sql propre au protocole POPReptile.
-- Ce fichier a été généré à partir d'une copie du fichier synthese_svo.sql du module monitoring
-- (Vue générique pour alimenter la synthèse dans le cadre d'un protocole site-visite-observation)
--
-- Le fichier sera joué à l'installation avec la valeur de module_code qui sera attribué automatiquement
-- Il contient une variable :module_code (ou :'module_code')
-- utiliser psql avec l'option -v module_code=<module_code
-- Ne pas remplacer cette variable, elle est indispensable pour les scripts d'installations
-- le module pouvant être installé avec un code différent de l'original

DROP VIEW IF EXISTS gn_monitoring.v_synthese_:module_code;

CREATE OR REPLACE VIEW gn_monitoring.v_synthese_:module_code
AS WITH source AS (
         SELECT id_source
           FROM gn_synthese.t_sources
          WHERE name_source = CONCAT('MONITORING_', UPPER(:'module_code'))
        )
 SELECT 
    o.uuid_observation AS unique_id_sinp,
    v.uuid_base_visit AS unique_id_sinp_grp,
    (SELECT id_source FROM source) AS id_source,
    v.id_module as id_module,
    o.id_observation AS entity_source_pk_value,
    v.id_dataset,
    ref_nomenclatures.get_id_nomenclature('NAT_OBJ_GEO', 'In') AS id_nomenclature_geo_object_nature,
    ref_nomenclatures.get_id_nomenclature('TYP_GRP', 'PASS') AS id_nomenclature_grp_typ,
    -- Methode d'observation à vue, que ça soit sous plaque ou entre les plaques
    ref_nomenclatures.get_id_nomenclature('METH_OBS', '0') AS id_nomenclature_obs_technique, 
    -- On ne renseigne pas le bio_status : on n'en dispose pas
    ref_nomenclatures.get_id_nomenclature('ETAT_BIO', '1') as id_nomenclature_bio_condition,
    -- On ne renseigne pas la naturalness : on n'en dispose pas
    -- On regarde s'il existe des médias pour transmettre les preuves
    CASE
	WHEN unique_id_media IS NOT NULL THEN ref_nomenclatures.get_id_nomenclature('PREUVE_EXIST', '1')
	ELSE ref_nomenclatures.get_id_nomenclature('PREUVE_EXIST', '2')
    END AS id_nomenclature_exist_proof,
    ref_nomenclatures.get_id_nomenclature('OBJ_DENBR', 'IND') AS id_nomenclature_obj_count,
    -- Remplacé par id_nomenclature_typ_denbr, mais il faut aussi traduire les anciennes données avant de MAJ les données !!
    nullif(json_extract_path(oc.data::json,'id_nomenclature_typ_denbr')::text, 'null')::integer AS id_nomenclature_type_count,
    --CASE
    --    WHEN json_extract_path(oc.data::json,'type_denombrement')::text = 'Compté' THEN ref_nomenclatures.get_id_nomenclature('TYP_DENBR'::character varying, 'Co'::character varying)
    --    WHEN json_extract_path(oc.data::json,'type_denombrement')::text = 'Estimé' THEN ref_nomenclatures.get_id_nomenclature('TYP_DENBR'::character varying, 'Es'::character varying)
    --    ELSE ref_nomenclatures.get_id_nomenclature('TYP_DENBR'::character varying, 'NSP'::character varying)
    --END AS id_nomenclature_type_count,
    CASE
	WHEN json_extract_path(oc.data::json, 'presence')::text = 'Non'
            THEN ref_nomenclatures.get_id_nomenclature('STATUT_OBS', 'No')
	ELSE ref_nomenclatures.get_id_nomenclature('STATUT_OBS', 'Pr')
    END as id_nomenclature_observation_status,
    ref_nomenclatures.get_id_nomenclature('STATUT_SOURCE', 'Te') AS id_nomenclature_source_status,
    ref_nomenclatures.get_id_nomenclature('TYP_INF_GEO', '1') AS id_nomenclature_info_geo_type,

CASE 
        WHEN json_extract_path(oc.data::json,'stade_vie')::text = 'Adultes' THEN ref_nomenclatures.get_id_nomenclature('STADE_VIE'::character varying, '2'::character varying)
        WHEN json_extract_path(oc.data::json,'stade_vie')::text IN ('Nouveaux-nés','Juvéniles') THEN ref_nomenclatures.get_id_nomenclature('STADE_VIE'::character varying, '3'::character varying)
        ELSE ref_nomenclatures.get_id_nomenclature('STADE_VIE'::character varying, '1'::character varying)
    END AS id_nomenclature_life_stage,
    -- XXX TODO : à reprendre (revoir avec Audrey / Florèn)
    CASE
        WHEN json_extract_path(oc.data::json,'type_denombrement')::text = 'Compté' AND nullif(json_extract_path(oc.data::json,'nombre_compte')::text,null)::integer IS NOT NULL THEN nullif(json_extract_path(oc.data::json,'nombre_compte')::text,null)::integer
        WHEN json_extract_path(oc.data::json,'type_denombrement')::text = 'Estimé' AND nullif(json_extract_path(oc.data::json,'nombre_estime_min')::text,null)::integer IS NOT NULL THEN nullif(json_extract_path(oc.data::json,'nombre_estime_min')::text,null)::integer
        ELSE 1
    END AS count_min,
    CASE
        WHEN json_extract_path(oc.data::json,'type_denombrement')::text = 'Compté' AND nullif(json_extract_path(oc.data::json,'nombre_compte')::text,null)::integer IS NOT NULL THEN nullif(json_extract_path(oc.data::json,'nombre_compte')::text,null)::integer
        WHEN json_extract_path(oc.data::json,'type_denombrement')::text = 'Estimé' AND nullif(json_extract_path(oc.data::json,'nombre_estime_max')::text,null)::integer IS NOT NULL THEN nullif(json_extract_path(oc.data::json,'nombre_estime_max')::text,null)::integer
        ELSE 1
    END AS count_max,
    o.cd_nom,
    t.nom_complet AS nom_cite,
    alt.altitude_min,
    alt.altitude_max,
    -- XXX TODO : vérifier la projection par défaut pour le module monitoring
    -- En fonction, adapter les 3 géométries ci-dessous
    s.geom AS the_geom_4326,
    st_centroid(s.geom) AS the_geom_point,
    s.geom_local AS the_geom_local,
    -- XXX TODO : voir si on ajoute les horaires de visite (champs complémentaires si remplis)
    v.visit_date_min AS date_min,
    v.visit_date_min AS date_max,
    obs.observers,
    v.id_digitiser,
    -- Examen visuel à "distance" (sans manipulation des individus)
    ref_nomenclatures.get_id_nomenclature('METH_DETERMIN', '18') AS id_nomenclature_determination_method,
    v.comments AS comment_context,
    o.comments AS comment_description,
    -- Les trois champs qui suivent ne sont pas utilisés mais peuvent faciliter les recherchers
    obs.ids_observers,
    v.id_base_site,
    v.id_base_visit,
    -- XXX TODO : à revoir
    json_build_object(
        'expertise_operateur', json_extract_path(tsg.data::json,'expertise')::text, 
        'nom_aire', tsg.sites_group_name, 
        'description_aire', tsg.sites_group_description, 
        'habitat_principal_aire', json_extract_path(tsg.data::json,'habitat_principal')::text, 
        'commentaire_aire', tsg.comments, 
        'nom_transect', s.base_site_name, 
	-- Vérifier comment ressort la méthode de prospection et si on souhaite la conserver
        'methode_prospection', json_extract_path(sc.data::json,'methode_prospection')::text, 
	-- XXX TODO : vérifier comment ressort l'information sur les milieux !
        'milieu_transect', json_extract_path(sc.data::json,'milieu_transect')::text, 
        'milieu_bordier', json_extract_path(sc.data::json,'milieu_bordier')::text, 
        'milieu_mosaique', json_extract_path(sc.data::json,'milieu_mosaique_vegetale')::text, 
        'milieu_homogene', json_extract_path(sc.data::json,'milieu_homogene')::text, 
        'milieu_anthropique', json_extract_path(sc.data::json,'milieu_anthropique')::text, 
        'commentaire_transect', json_extract_path(sc.data::json,'comment')::text, 
        'num_passage', json_extract_path(vc.data::json,'num_passage')::text, 
        'heure_debut', json_extract_path(vc.data::json,'Heure_debut')::text, 
        'heure_fin', json_extract_path(vc.data::json,'Heure_fin')::text, 
        'abondance', json_extract_path(oc.data::json,'abondance')::text
        ) as additional_data
   FROM gn_monitoring.t_base_visits v
     JOIN gn_monitoring.t_visit_complements vc on v.id_base_visit = vc.id_base_visit 
     JOIN gn_monitoring.t_base_sites s ON s.id_base_site = v.id_base_site
     JOIN gn_monitoring.t_site_complements sc on sc.id_base_site = s.id_base_site
     JOIN gn_monitoring.t_sites_groups tsg ON sc.id_sites_group = tsg.id_sites_group
     JOIN gn_commons.t_modules m ON m.id_module = v.id_module
     JOIN gn_monitoring.t_observations o ON o.id_base_visit = v.id_base_visit
     JOIN gn_monitoring.t_observation_complements oc ON oc.id_observation = o.id_observation
     LEFT JOIN gn_commons.t_medias tm ON (tm.id_table_location = gn_commons.get_table_location_id('gn_monitoring', 't_observations') AND tm.uuid_attached_row = o.uuid_observation)
     JOIN taxonomie.taxref t ON t.cd_nom = o.cd_nom
     LEFT JOIN LATERAL ( SELECT array_agg(r.id_role) AS ids_observers,
            string_agg(concat(r.nom_role, ' ', r.prenom_role), ' ; '::text) AS observers
           FROM gn_monitoring.cor_visit_observer cvo
             JOIN utilisateurs.t_roles r ON r.id_role = cvo.id_role
          WHERE cvo.id_base_visit = v.id_base_visit) obs ON true
     LEFT JOIN LATERAL ref_geo.fct_get_altitude_intersection(s.geom_local) alt(altitude_min, altitude_max) ON true
    WHERE m.module_code = :'module_code';
