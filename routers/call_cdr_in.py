from fastapi import APIRouter, Query, HTTPException, Depends, Request, Header
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Optional
from datetime import date, datetime, timedelta
from pydantic import BaseModel
from urllib.request import Request
from typing import Optional

router = APIRouter()

# First DB connection
DATABASE_URL = "mysql+pymysql://root:dial%40mas123@192.168.10.12/db_dialdesk?charset=utf8mb4"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Second DB connection
DATABASE_URL1 = "mysql+pymysql://root:vicidialnow@192.168.10.5/asterisk?charset=utf8mb4"
engine1 = create_engine(DATABASE_URL1, echo=True)
SessionLocal1 = sessionmaker(autocommit=False, autoflush=False, bind=engine1)


def get_db1():
    db = SessionLocal1()
    try:
        yield db
    finally:
        db.close()


@router.get("/call_cdr_in/")
def get_call_cdr_in(
    from_date: date,
    to_date: date,
    client_id: str = Query(..., alias="clientId"),
    authorization: str = Header(None),
    category_qry: Optional[str] = "",  # Optional query param
    db1: Session = Depends(get_db1),
    db2: Session = Depends(get_db)
):

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    token = authorization.split(" ")[1]
    try:
        if client_id == "All":
            db2.execute(text("SET SESSION group_concat_max_len = 20000"))
            result = db2.execute(
                text(f"""
                    SELECT GROUP_CONCAT(campaignid) AS campaign_id 
                    FROM registration_master 
                    WHERE status = 'A' AND is_dd_client = '1' {category_qry}
                """)
            ).fetchone()

            campaign_ids = result.campaign_id
            if not campaign_ids:
                raise HTTPException(status_code=404, detail="No campaigns found for All clients")
            campaign_filter = f"t2.campaign_id IN ({campaign_ids})"
        else:
            result = db2.execute(
                text(f"""
                    SELECT campaignid 
                    FROM registration_master 
                    WHERE company_id = :client_id {category_qry}
                """),
                {"client_id": client_id}
            ).fetchone()

            if not result:
                raise HTTPException(status_code=404, detail="Client not found")
            campaign_ids = result.campaignid
            campaign_filter = f"t2.campaign_id IN ({campaign_ids})"

        query = f"""
            SELECT t2.uniqueid, SEC_TO_TIME(t6.p) AS ParkedTime, t2.campaign_id, 
                IF(queue_seconds <= 20, 1, 0) AS Call20,
                IF(queue_seconds <= 60, 1, 0) AS Call60,
                IF(queue_seconds <= 90, 1, 0) AS Call90,
                t2.user AS Agent, vc.full_name, t2.lead_id AS LeadId,
                RIGHT(phone_number, 10) AS PhoneNumber,
                DATE(call_date) AS CallDate, SEC_TO_TIME(queue_seconds) AS QueueTime,
                IF(queue_seconds = 0, FROM_UNIXTIME(t2.start_epoch), FROM_UNIXTIME(t2.start_epoch - queue_seconds)) AS QueueStart,
                FROM_UNIXTIME(t2.start_epoch) AS StartTime,
                FROM_UNIXTIME(t2.end_epoch) AS EndTime,
                SEC_TO_TIME(IFNULL(t3.talk_sec, t2.length_in_sec)) AS CallDuration,
                IFNULL(t3.talk_sec, t2.length_in_sec) AS CallDuration1,
                FROM_UNIXTIME(
                    t2.end_epoch + TIME_TO_SEC(
                        IF(t3.dispo_sec IS NULL, SEC_TO_TIME(0),
                        IF(t3.sub_status IN ('LOGIN', 'Feed') OR t3.talk_sec = t3.dispo_sec OR t3.talk_sec = 0,
                        SEC_TO_TIME(1),
                        IF(t3.dispo_sec > 100, SEC_TO_TIME(t3.dispo_sec - (t3.dispo_sec / 100) * 100),
                        SEC_TO_TIME(t3.dispo_sec)))))
                ) AS WrapEndTime,
                IF(t3.dispo_sec IS NULL, SEC_TO_TIME(0),
                IF(t3.sub_status IN ('LOGIN', 'Feed') OR t3.talk_sec = t3.dispo_sec OR t3.talk_sec = 0,
                SEC_TO_TIME(1),
                IF(t3.dispo_sec > 100, SEC_TO_TIME(t3.dispo_sec - (t3.dispo_sec / 100) * 100),
                SEC_TO_TIME(t3.dispo_sec)))) AS WrapTime,
                sub_status, t2.status, t2.term_reason, t2.xfercallid
            FROM vicidial_closer_log t2
            LEFT JOIN vicidial_agent_log t3 ON t2.uniqueid = t3.uniqueid AND t2.user = t3.user
            LEFT JOIN (
                SELECT uniqueid, SUM(parked_sec) AS p 
                FROM park_log 
                WHERE STATUS = 'GRABBED' AND DATE(parked_time) BETWEEN :from_date AND :to_date 
                GROUP BY uniqueid
            ) t6 ON t2.uniqueid = t6.uniqueid
            LEFT JOIN vicidial_users vc ON t2.user = vc.user
            WHERE DATE(t2.call_date) BETWEEN :from_date AND :to_date 
            AND {campaign_filter} 
            AND t2.lead_id IS NOT NULL
        """

        results = db1.execute(text(query), {"from_date": from_date, "to_date": to_date}).fetchall()
        data = [dict(row._mapping) for row in results]

        return {"status": "success", "data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/call_cdr_ob/")
def get_call_cdr_ob(
        from_date: date,
        to_date: date,
        client_id: str = Query(..., alias="clientId"),
        authorization: str = Header(None),
        category_qry: Optional[str] = "",
        db1: Session = Depends(get_db1),
        db2: Session = Depends(get_db)
):

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    token = authorization.split(" ")[1]
    try:
        if client_id == "All":
            db2.execute(text("SET SESSION group_concat_max_len = 20000"))
            result = db2.execute(
                text(f"""
                    SELECT GROUP_CONCAT(campaignid) AS campaign_id 
                    FROM registration_master 
                    WHERE status = 'A' AND is_dd_client = '1' {category_qry}
                """)
            ).fetchone()

            campaign_ids = result.campaign_id
            if not campaign_ids:
                raise HTTPException(status_code=404, detail="No campaigns found for All clients")
            campaign_filter = f"t2.campaign_id IN ({campaign_ids})"
        else:
            result = db2.execute(
                text(f"""
                    SELECT campaignid 
                    FROM registration_master 
                    WHERE company_id = :client_id {category_qry}
                """),
                {"client_id": client_id}
            ).fetchone()

            if not result:
                raise HTTPException(status_code=404, detail="Client not found")
            campaign_ids = result.campaignid
            campaign_filter = f"t2.campaign_id IN ({campaign_ids})"

        query = f"""
            SELECT DATE(t2.call_date) AS CallDate,FROM_UNIXTIME(t2.start_epoch) AS StartTime,FROM_UNIXTIME(t2.end_epoch) AS Endtime,LEFT(t2.phone_number,10) AS PhoneNumber,
t2.`user` AS Agent,vu.full_name as full_name,if(t2.`user`='VDAD','Not Connected','Connected') calltype,t2.status as status,if(t2.`list_id`='998','Mannual','Auto') dialmode,t2.campaign_id as campaign_id,t2.lead_id as lead_id,
 t2.length_in_sec AS LengthInSec,
            SEC_TO_TIME(t2.length_in_sec) AS LengthInMin,
            t2.length_in_sec AS CallDuration,
            t2.`status` AS CallStatus,
            t3.`pause_sec` AS PauseSec,
            t3.`wait_sec` AS WaitSec,
            t3.`talk_sec` AS TalkSec,t3.dispo_sec AS DispoSec
  FROM asterisk.vicidial_log t2
            LEFT JOIN vicidial_agent_log t3 ON t2.uniqueid=t3.uniqueid left join vicidial_users vu on t2.user=vu.user
            WHERE DATE(t2.call_date) BETWEEN :from_date AND :to_date 
             and DATE(t2.call_date) BETWEEN DATE_SUB(CURDATE(), INTERVAL 3 MONTH) AND CURDATE() AND {campaign_filter}
            AND t2.lead_id IS NOT NULL
        """

        results = db1.execute(text(query), {"from_date": from_date, "to_date": to_date}).fetchall()
        data = [dict(row._mapping) for row in results]

        return {"status": "success", "data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def run_raw_query(db: Session, query: str):
    try:
        result = db.execute(text(query))
        keys = result.keys()
        rows = result.fetchall()
        return [dict(zip(keys, row)) for row in rows]
    except Exception as e:
        print(f"Query Error: {e}")
        return []


@router.get("/reportprint")
def report_print(
        from_date: date,
        to_date: date,
        client_id: str = Query(..., alias="clientId"),
        authorization: str = Header(None),
        category_qry: Optional[str] = "",
        db1: Session = Depends(get_db1),
        db: Session = Depends(get_db)
):
    # If the client_id is "All", we select all clients
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    token = authorization.split(" ")[1]

    if client_id == "All":
        campaign_ids_query = f"""
            SELECT GROUP_CONCAT(campaignid) AS campaign_id 
            FROM registration_master 
            WHERE status='A' AND is_dd_client='1'
        """
        result = run_raw_query(db, campaign_ids_query)
        campaign_id_str = result[0]["campaign_id"]
        campaign_filter = f"t2.campaign_id in ({campaign_id_str})"

        client_ids_query = f"""
            SELECT GROUP_CONCAT(company_id) AS company_id 
            FROM registration_master 
            WHERE status='A' AND is_dd_client='1'
        """
        result2 = run_raw_query(db, client_ids_query)
        client_list_str = f"ClientId in ({result2[0]['company_id']})"
    else:
        client_filter = f"company_id='{client_id}' "
        client_data = run_raw_query(db, f"SELECT campaignid FROM registration_master WHERE {client_filter}")
        campaign_id_str = client_data[0]['campaignid']
        campaign_filter = f"t2.campaign_id in ({campaign_id_str})"
        client_list_str = f"ClientId in ({client_id})"

    # Convert from_date and to_date to datetime objects
    from_date = datetime.strptime(f"{from_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
    to_date = datetime.strptime(f"{to_date} 23:59:59", "%Y-%m-%d %H:%M:%S")

    response_data = {}

    # Loop through each hour between the from_date and to_date
    while from_date < to_date:
        time_label = from_date.strftime("%H")
        date_label = from_date.strftime("%Y-%m-%d")

        start_time = from_date
        end_time = start_time + timedelta(hours=1)

        # Build SQL query
        query = f"""
        SELECT COUNT(*) AS Total,
            SUM(IF(t2.user!='VDCL' AND t2.queue_seconds<=20,1,0)) AS WIthinSLA,
            SUM(IF(t2.user!='VDCL',1,0)) AS Answered,
            COUNT(DISTINCT IF(t2.user != 'vdcl', t2.user, NULL)) AS Manpower,
            SUM(IF(t2.user!='VDCL',t2.talk_sec,0)) AS Talk,
            SUM(IF(t2.user!='VDCL' AND t2.queue_seconds<=20,t2.queue_seconds,0)) AS wait,
            SUM(IF(t2.user!='VDCL',t2.dispo_sec,0)) AS dispo,
            SUM(IF(t2.user!='VDCL',t2.pause_sec,0)) AS pause,
            SUM(IF(t2.user!='VDCL',t2.hold_sec,0)) AS hold,
            SUM(IF(t2.user!='VDCL' AND t2.queue_seconds<=20,t2.al_sec,0)) AS Al,
            SUM(IF(t2.user!='VDCL',t2.login_sec,0)) AS Total login,
            SUM(IF(t2.user!='VDCL',t2.net_login_sec,0)) AS Net login,
            SUM(IF(t2.user!='VDCL',t2.util_sec,0)) AS Utilization
        FROM asterisk.vicidial_closer_log t2
        LEFT JOIN asterisk.vicidial_users vu ON t2.user = vu.user
        LEFT JOIN asterisk.vicidial_agent_log t1 ON t1.uniqueid=t2.uniqueid AND t2.user=t1.user
        LEFT JOIN (
            SELECT uniqueid, SUM(parked_sec) AS p 
            FROM park_log 
            WHERE STATUS='GRABBED' AND parked_time >= '{start_time}' AND parked_time < '{end_time}'
            GROUP BY uniqueid
        ) t3 ON t1.uniqueid = t3.uniqueid
        WHERE t2.call_date >= '{start_time}' AND t2.call_date < '{end_time}' AND {campaign_filter}
        """

        hourly_result = run_raw_query(db, query)

        # Check if the result is not empty
        if hourly_result:
            hourly_result = hourly_result[0]
        else:
            # Handle the case where no data is returned (e.g., set default values)
            hourly_result = {
                "Total": 0,
                "Answered": 0,
                "Manpower": 0,
                "Talk": 0,
                "Wait": 0,
                "Dispo": 0,
                "Pause": 0,
                "Hold": 0,
                "Al": 0,
                "Total login": 0,
                "Net login": 0,
                "Utilization": 0,
                "WIthinSLA": 0
            }
        if date_label not in response_data:
            response_data[date_label] = {}

        if time_label not in response_data[date_label]:
            response_data[date_label][time_label] = {}
        # Now you can safely use hourly_result
        response_data[date_label][time_label] = {
            "Total": hourly_result["Total"],
            "Answered": hourly_result["Answered"],
            "Manpower": hourly_result["Manpower"],
            "Talk": hourly_result.get("Talk", 0),
            "Wait": hourly_result.get("wait", 0),
            "Dispo": hourly_result.get("dispo", 0),
            "Pause": hourly_result.get("pause", 0),
            "Hold": hourly_result.get("hold", 0),
            "Al %": round(hourly_result.get("Al", 0), 2),
            "SL %": round((hourly_result["WIthinSLA"] / hourly_result["Answered"]) * 100, 2) if hourly_result[
                "Answered"] else 0,
            "Total login": hourly_result.get("Total login", 0),
            "Net login": hourly_result.get("Net login", 0),
            "Utilization %": round(hourly_result.get("Utilization", 0), 2),
        }

        from_date = end_time

    return {"status": "success", "data": response_data}