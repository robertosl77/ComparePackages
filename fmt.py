from oracle import OracleDB

class FMT:
    def __init__(self):
        self.db = OracleDB()

    def connect(self):
        self.db.connect()

    def close(self):
        self.db.close()

    def get_data(self, query):
        return self.db.execute_query(query)

    def get_nuevo_data(self, documento=''):
        params = '' if documento == '' else f" where nro_documento='{documento}'"
        query = f"""SELECT ID, NRO_DOCUMENTO DOCUMENTO, OBJECTID, NEXUS_GIS.INFORMACION_ENREMTAT_DIN.CT_FROM_OBJECTID(OBJECTID) CT, CANT_AFECTACIONES AFECTADOS 
                    FROM (SELECT DISTINCT D.ID, D.NAME NRO_DOCUMENTO, OB.OBJECTID, DECODE(DT.IS_FORCED, 0,'PROGRAMADO','FORZADO') ORIGEN, 
                    ARO.TIME AS FECHA_DOCUMENTO, DS.DESCRIPTION AS ESTADO, 
                    CASE WHEN D.LAST_STATE_ID>=5 THEN 0 ELSE AE.COUNT_PROPERTIES END CANT_AFECTACIONES, 
                    NVL(D.ESTIMATED_RESTORATION_TIME,ROUND(ARO.TIME + (4 / 24), 'HH24')) FECHA_NORM_PREVISTA, 'MT' SISTEMA , 
                    D.MODEL_ELEMENT_CHAIN CAD_ELECTRICA, 'MT FORZADA' MOTIVO , 
                    (SELECT FAILURE_TYPE_ID FROM NEXUS_GIS.OMS_DOCUMENT_CAUSE WHERE DOCUMENT_ID = D.ID) CAUSA , D.OWNER_GROUP 
                    FROM NEXUS_GIS.OMS_DOCUMENT D, NEXUS_GIS.OMS_OPERATION OP, NEXUS_GIS.OMS_DOCUMENT_TYPE DT, NEXUS_GIS.OMS_DOCUMENT_STATE DS, 
                    NEXUS_GIS.SPRGOPERATIONS SO, NEXUS_GIS.OMS_AFFECT_RESTORE_OPERATION ARO, NEXUS_GIS.OMS_AFFECTED_ELEMENT AE , NEXUS_GIS.SPROBJECTS OB 
                    WHERE 1=1 AND D.ID = OP.DOCUMENT_ID AND D.TYPE_ID = DT.ID AND D.LAST_STATE_ID = DS.ID AND OP.OPERATION_MODEL_ID = SO.OPERATIONID 
                    AND OP.ID = ARO.OPERATION_ID AND ARO.ID = AE.AFFECT_ID AND AE.ELEMENT_ID = OB.OBJECTID AND SO.LOGIDTO = 0 
                    AND D.LAST_STATE_ID IN (2,3,4) AND (D.NOTES NOT LIKE '%revisado..%' OR D.NOTES IS NULL) AND ((SYSDATE - ARO.TIME) * (60 * 24))>=10 
                    AND (SELECT COUNT(1) FROM NEXUS_GIS.TABLA_ENREMTAT_DET WHERE ELEMENT_ID=OB.OBJECTID AND CANT_AFECTACIONES>0)=0 
                    AND ARO.IS_RESTORE = 0 AND D.TYPE_ID IN (3) AND AE.COUNT_PROPERTIES>0 
                    AND (SELECT COUNT(1) FROM NEXUS_GIS.TABLA_ENREMTAT_DET WHERE ID=D.ID)=0 
                    AND AE.AFFECT_ID =(SELECT MAX(AFFECT_ID) FROM NEXUS_GIS.OMS_AFFECTED_ELEMENT AE1, NEXUS_GIS.OMS_AFFECT_RESTORE_OPERATION RO 
                    WHERE AE1.AFFECT_ID = RO.ID AND RO.DOCUMENT_ID = D.ID AND AE1.ELEMENT_ID = AE.ELEMENT_ID) ORDER BY 1 ){params}"""
        return self.get_data(query)

    def get_viejo_data2(self, documento=''):
        params = '' if documento == '' else f" AND D.NAME='{documento}'"
        plsql = f"""
        DECLARE
            CURSOR cur_doc_gen IS
                SELECT G.*
                FROM
                    NEXUS_GIS.TABLA_ENREMTAT_GEN G,
                    NEXUS_GIS.OMS_DOCUMENT D
                WHERE
                    G.ORIGEN = 'FORZADO'
                    AND  D.ID = G.ID
                    AND  D.LAST_STATE_ID>1
                    AND  D.LAST_STATE_ID<5
                    AND  D.ID NOT IN (SELECT ID FROM NEXUS_GIS.TABLA_ENREMTAT_DET)
                    AND (D.NOTES NOT LIKE '%revisado..%' OR D.NOTES IS NULL)
                    {params};
            CURSOR cur_doc_det (p_doc_id NUMBER) IS
                SELECT DISTINCT
                    OB.OBJECTID,
                    D.ID,
                    D.NAME nro_documento,
                    DECODE(IS_FORCED, 0,'PROGRAMADO','FORZADO') ORIGEN,
                    ARO.TIME AS FECHA_DOCUMENTO,
                    DS.DESCRIPTION AS ESTADO,
                    AE.COUNT_PROPERTIES CANT_AFECTACIONES,
                    D.ESTIMATED_RESTORATION_TIME FECHA_NORM_PREVISTA,
                    DECODE(D.TYPE_ID ,3,'MT',4,'MT',5,'AT',6,'AT') SISTEMA,
                    D.MODEL_ELEMENT_CHAIN CAD_ELECTRICA,
                    DECODE(D.TYPE_ID ,3,'MT Forzada',4,'MT Forzada',5,'AT',6,'AT') MOTIVO,
                    D.OWNER_GROUP
                FROM
                    NEXUS_GIS.OMS_DOCUMENT D,
                    NEXUS_GIS.OMS_DOCUMENT_TYPE DT,
                    NEXUS_GIS.OMS_DOCUMENT_STATE DS,
                    NEXUS_GIS.OMS_OPERATION OP,
                    NEXUS_GIS.SPRGOPERATIONS O,
                    NEXUS_GIS.OMS_AFFECT_RESTORE_OPERATION ARO,
                    NEXUS_GIS.OMS_AFFECTED_ELEMENT AE,
                    NEXUS_GIS.SPROBJECTS OB
                WHERE
                    D.ID IN ( p_doc_id)
                    AND D.TYPE_ID = DT.ID
                    AND D.LAST_STATE_ID = DS.ID
                    AND D.ID = OP.DOCUMENT_ID
                    AND OP.OPERATION_MODEL_ID = O.OPERATIONID
                    AND OP.ID = ARO.OPERATION_ID
                    AND ARO.ID = AE.AFFECT_ID
                    AND AE.ELEMENT_ID = OB.OBJECTID
                    AND D.TYPE_ID IN (3,4)
                    AND D.LAST_STATE_ID>1
                    AND D.LAST_STATE_ID<5
                    AND ((sysdate - ARO.TIME) * (60 * 24))>=(SELECT VALOR FROM NEXUS_GIS.PARAM_ENRE_MTAT WHERE TIPO='DURACION')
                    AND ARO.IS_RESTORE = 0
                    AND O.LOGIDTO = 0
                ORDER BY 1;
            v_fecha_norm    NEXUS_GIS.OMS_DOCUMENT.LAST_STATE_CHANGE_TIME%TYPE;
            v_streetid      NUMBER;
            v_calle         VARCHAR2(200):= NULL;
            v_entrecalle    VARCHAR2(200):= NULL;
            v_localidad     VARCHAR2(200):= NULL;
            v_partido       VARCHAR2(200):= NULL;
            v_zona          VARCHAR2(200):= NULL;
            v_delta         VARCHAR2(200):= NULL;
            v_coordx        NEXUS_BDT.BDTH_CENTROSTRANSFORMACION.X%TYPE;
            v_coordy        NEXUS_BDT.BDTH_CENTROSTRANSFORMACION.Y%TYPE;
            v_row_log       NEXUS_GIS.WS_ENREMTAT_LOG%ROWTYPE;
            v_row_logct     NEXUS_GIS.WS_ENRE_LOG_CT%ROWTYPE;
            v_resp          VARCHAR2(20);
            v_ct_afectado   VARCHAR2(100);
            v_cant_ct_rep   NUMBER;
            v_ct_afec_flag  NUMBER;
            v_otro_doc      NEXUS_GIS.OMS_DOCUMENT.NAME%TYPE;
            v_confirmado    NUMBER;
            v_coord_res     VARCHAR2(10);
            v_logct         NUMBER;
            cur_output SYS_REFCURSOR;
        BEGIN
            nexus_gis.INFORMACION_ENREMTAT.INSERTA_DOCUMENTOS_GEN;
            v_row_logct.operacion:='INSERTA_DOCUMENTOS_DET';
            OPEN cur_output FOR
            SELECT
                REG.ID,
                REG.NRO_DOCUMENTO,
                REG.OBJECTID,
                V_CT_AFECTADO,
                REG.CANT_AFECTACIONES
            FROM
                NEXUS_GIS.TABLA_ENREMTAT_GEN G,
                NEXUS_GIS.OMS_DOCUMENT D;
            
            FOR c_doc IN cur_doc_gen LOOP
                v_row_logct.descripcion:=NULL;
                FOR reg IN cur_doc_det (c_doc.id) LOOP
                    v_row_logct.document_id:=reg.id;
                    v_row_logct.objectid:=reg.objectid;
                    v_row_logct.afectados:=reg.cant_afectaciones;
                    v_fecha_norm:=NULL;
                    BEGIN
                        v_ct_afectado:= NEXUS_GIS.INFORMACION_ENREMTAT.CT_FROM_OBJECTID(reg.objectid);
                    EXCEPTION
                        WHEN OTHERS THEN v_ct_afectado:=0;
                    END;
                    v_row_logct.ct_afectado:=v_ct_afectado;
                    BEGIN
                        SELECT nro_documento, COUNT(1) INTO v_otro_doc, v_ct_afec_flag FROM NEXUS_GIS.TABLA_ENREMTAT_DET WHERE element_id =reg.objectid AND cant_afectaciones >0 AND ROWNUM=1 GROUP BY nro_documento;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_otro_doc:=NULL;
                            v_ct_afec_flag:=0;
                    END;
                    BEGIN
                        SELECT COUNT(1) INTO v_cant_ct_rep FROM NEXUS_GIS.TABLA_ENREMTAT_DET WHERE nro_documento =reg.nro_documento AND element_id =reg.objectid;
                    EXCEPTION
                        WHEN OTHERS THEN v_cant_ct_rep:=-1;
                    END;
                    BEGIN
                        SELECT idoc.evento_confirmado INTO v_confirmado
                        FROM NEXUS_GIS.IDMS_DOCUMENTO_INFO idoc, NEXUS_GIS.OMS_DOCUMENT doc
                        WHERE
                            idoc.id_documento = doc.id
                            AND doc.id =c_doc.id;
                    EXCEPTION
                        WHEN OTHERS THEN v_confirmado:= 0;
                    END;
                    v_streetid:=NEXUS_GIS.INFORMACION_ENREMTAT.FULL_ADDRESS(
                                                    v_ct_afectado,
                                                    v_entrecalle,
                                                    v_calle,
                                                    v_localidad,
                                                    v_partido,
                                                    v_zona,
                                                    v_delta);
                    IF NOT LENGTH(v_ct_afectado) > 1 THEN
                        v_row_logct.descripcion:='No tiene el largo correcto. ';
                    ELSIF v_ct_afec_flag > 0 THEN
                        v_row_logct.descripcion:='CT se encuentra en '||v_otro_doc||'. ';
                    ELSIF v_cant_ct_rep<>0 THEN
                        v_row_logct.descripcion:='CT/OBJECTID existente en el mismo documento. ';
                    ELSE
                        v_row_logct.descripcion:='CT Insertado Correctamente. ';
                        v_coord_res:=NEXUS_GIS.INFORMACION_ENREMTAT.COORDENADAS(
                                                            v_ct_afectado,
                                                            v_coordx,
                                                            v_coordy);
                        BEGIN
                            IF reg.origen = 'FORZADO' THEN
                                v_fecha_norm:=ROUND(reg.fecha_documento + 4 / 24, 'HH24');
                            END IF;
                        EXCEPTION
                            WHEN OTHERS THEN v_fecha_norm:=ROUND(SYSDATE + 4 / 24, 'HH24');
                        END;
                        DBMS_OUTPUT.PUT_LINE(
                        reg.id
                        ||'|'||reg.nro_documento
                        ||'|'||reg.objectid
                        ||'|'||v_ct_afectado
                        ||'|'||reg.cant_afectaciones
                        );
                    END IF;
                END LOOP;
            END LOOP;
            :cursor := cur_output;
        END;
        """
        print(plsql)
        return self.db.execute_plsql(plsql)

    def get_viejo_data_plsql(self, documento=''):
        params = '' if documento == '' else f" where documento='{documento}'"
        plsql = f"""
        DECLARE
            v_id NUMBER;
            v_documento VARCHAR2(255);
            v_objectid NUMBER;
            v_ct NUMBER;
            v_afectados NUMBER;
        BEGIN
            v_id := 11853047;
            v_documento := 'D-24-02-035951';
            v_objectid := 61747196;
            v_ct := 79161;
            v_afectados := 383;
            OPEN :cursor FOR
            --test
            SELECT v_id AS id, v_documento AS documento, v_objectid AS objectid, v_ct AS ct, v_afectados AS afectados FROM DUAL;
        END;
        """
        return self.db.execute_plsql(plsql)
    
    def get_viejo_data(self, documento=''):
        params = '' if documento == '' else f"{documento}"
        # query = f"""SELECT 11853047 AS id, 'D-24-02-035951' AS documento, 61747196 AS objectid, 79161 AS ct, 383 AS afectados FROM DUAL"""
        query = f"""
            SELECT ID, NRO_DOCUMENTO DOCUMENTO, OBJECTID, NEXUS_GIS.INFORMACION_ENREMTAT_DIN.CT_FROM_OBJECTID(OBJECTID) CT, CANT_AFECTACIONES AFECTADOS FROM (
                SELECT DISTINCT
                    OB.OBJECTID,
                    D.ID,
                    D.NAME nro_documento,
                    DECODE(IS_FORCED, 0,'PROGRAMADO','FORZADO') ORIGEN,
                    ARO.TIME AS FECHA_DOCUMENTO,
                    DS.DESCRIPTION AS ESTADO,
                    AE.COUNT_PROPERTIES CANT_AFECTACIONES,
                    D.ESTIMATED_RESTORATION_TIME FECHA_NORM_PREVISTA,
                    DECODE(D.TYPE_ID ,3,'MT',4,'MT',5,'AT',6,'AT') SISTEMA,
                    D.MODEL_ELEMENT_CHAIN CAD_ELECTRICA,
                    DECODE(D.TYPE_ID ,3,'MT Forzada',4,'MT Forzada',5,'AT',6,'AT') MOTIVO,
                    D.OWNER_GROUP
                FROM
                    NEXUS_GIS.OMS_DOCUMENT D,
                    NEXUS_GIS.OMS_DOCUMENT_TYPE DT,
                    NEXUS_GIS.OMS_DOCUMENT_STATE DS,
                    NEXUS_GIS.OMS_OPERATION OP,
                    NEXUS_GIS.SPRGOPERATIONS O,
                    NEXUS_GIS.OMS_AFFECT_RESTORE_OPERATION ARO,
                    NEXUS_GIS.OMS_AFFECTED_ELEMENT AE,
                    NEXUS_GIS.SPROBJECTS OB
                WHERE
                    D.NAME = '{params}'
                    AND D.TYPE_ID = DT.ID
                    AND D.LAST_STATE_ID = DS.ID
                    AND D.ID = OP.DOCUMENT_ID
                    AND OP.OPERATION_MODEL_ID = O.OPERATIONID
                    AND OP.ID = ARO.OPERATION_ID
                    AND ARO.ID = AE.AFFECT_ID
                    AND AE.ELEMENT_ID = OB.OBJECTID
                    AND D.TYPE_ID IN (3,4)
                    AND D.LAST_STATE_ID>1
                    AND D.LAST_STATE_ID<5
                    AND ((sysdate - ARO.TIME) * (60 * 24))>=(SELECT VALOR FROM NEXUS_GIS.PARAM_ENRE_MTAT WHERE TIPO='DURACION')
                    AND ARO.IS_RESTORE = 0
                    AND O.LOGIDTO = 0
                ORDER BY 1
            )
        """
        return self.get_data(query)    