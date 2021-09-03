import requests
import lxml.html
import pandas as pd
from sodapy import Socrata
import json

def get_cdc():
    #these may also be important? https://www.cdc.gov/brfss/data_documentation/index.htm I'm not sure
    
    secrets = json.load(open("secrets.json", "r"))
    client = Socrata("chronicdata.cdc.gov",
                     secrets["cdc_app_token"],
                     username=secrets["socrata_username"],
                     password=secrets["socrata_password"])

    # source: https://chronicdata.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-H/iuq5-y9ct
    results = client.get("iuq5-y9ct")
    historical_questions_df = pd.DataFrame.from_records(results)

    # source: https://chronicdata.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-P/dttw-5yxu
    results = client.get("dttw-5yxu")
    prevalence_data_df = pd.DataFrame.from_records(results)

    # source: https://chronicdata.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factors-Selected-Metropolitan-Area/j32a-sa6u
    results = client.get("j32a-sa6u")
    smart_2011_to_present_df = pd.DataFrame.from_records(results)

    # source: https://chronicdata.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factors-Selected-Metropolitan-Area/waxm-p5qv
    results = client.get("waxm-p5qv")
    smart_2010_and_prior_df = pd.DataFrame.from_records(results)

    # source: https://chronicdata.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factors-Selected-Metropolitan-Area/cpem-dkkm
    results = client.get("cpem-dkkm")
    smart_county_df = pd.DataFrame.from_records(results)

    # source: https://chronicdata.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-A/d2rk-yvas
    results = client.get("d2rk-yvas")
    age_prevalence_2011_to_present_df = pd.DataFrame.from_records(results)

    # source: https://chronicdata.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factor-Surveillance-System-BRFSS-P/y4ft-s73h
    results = client.get("y4ft-s73h")
    prevalence_2010_and_prior_df = pd.DataFrame.from_records(results)

    # source: https://chronicdata.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factors-Selected-Metropolitan-Area/acme-vg9e
    results = client.get("acme-vg9e")
    smart_county_2010_and_prior_df = pd.DataFrame.from_records(results)

    # source: https://chronicdata.cdc.gov/Behavioral-Risk-Factors/Behavioral-Risk-Factors-Selected-Metropolitan-Area/at7e-uhkc
    results = client.get("at7e-uhkc")
    smart_age_2011_to_present_df = pd.DataFrame.from_records(results)
    return {
        "historical_questions.csv": historical_questions_df,
        "prevalence_data.csv": prevalence_data_df,
        "smart_2011_to_present.csv": smart_2011_to_present_df,
        "smart_2010_and_prior.csv": smart_2010_and_prior_df,
        "smart_county": smart_county_df,
        "age_prevalence_2011_to_present.csv": age_prevalence_2011_to_present_df,
        "prevalence_2021_and_prior.csv": prevalence_2010_and_prior_df,
        "smart_county_2010_and_prior.csv": smart_county_2010_and_prior_df,
        "smart_age_2011_to_present.csv": smart_age_2011_to_present_df
    }
        
def get_file(url, filename):
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)
            
def get_hrsa():
    #source: https://data.hrsa.gov/data/download?data=AHRF#AHRF
    get_file(
        "https://data.hrsa.gov//DataDownload/AHRF/AHRF_2019-2020_SAS.zip",
        "AHRF_2019_2020_SAS.zip"
    )
    get_file(
        "https://data.hrsa.gov//DataDownload/AHRF/AHRF_SN_2019-2020_SAS.zip",
        "AHRF_SN_2019_2020_SAS.zip"
    )
    get_file(
        "https://data.hrsa.gov//DataDownload/AHRF/AHRF_2018-2019_SAS.zip",
        "AHRF_2018_2019_SAS.zip"
    )
    get_file(
        "https://data.hrsa.gov//DataDownload/AHRF/AHRF_SN_2018-2019_SAS.zip",
        "AHRF_SN_2018_2019_SAS.zip"
    )

def cdc_wonder():
    # https://wonder.cdc.gov/
    # This 'database' is a giant pain in the ass.
    # going to have to script some sort of automated
    # api thing to get all the data out of this.
    pass

def clinical_trials_gov():
    # https://clinicaltrials.gov/
    # this is just research papers
    # we might be able to do some web scraping here
    # since they have already summarized results?
    # definitely a candidate for NLP
    pass

def mitracking():
    # https://mitracking.state.mi.us/
    # This 'database' is a giant pain in the ass.
    # going to have to script some sort of automated
    # api thing to get all the data out of this.
    pass

def public_use_data():
    # https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2019
    # xport needed, has issues.
    # will resolve if needed.
    pass

def epa_data():
    #https://www.epa.gov/toxics-release-inventory-tri-program/tri-data-and-tools
    # This 'database' is a giant pain in the ass.
    # going to have to script some sort of automated
    # api thing to get all the data out of this.
    pass

def healthdata_gov():
    secrets = json.load(open("secrets.json", "r"))
    client = Socrata("chronicdata.cdc.gov",
                     secrets["cdc_app_token"],
                     username=secrets["socrata_username"],
                     password=secrets["socrata_password"])

    # source: https://healthdata.gov/dataset/Screened-in-and-Screened-out-Referrals/k5kg-jgj9
    results = client.get("k5kg-jgj9")
    child_protective_services_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/dataset/Perpetrators-by-Relationship-to-Their-Victims/tw7x-jbvq
    results = client.get("tw7x-jbvq")
    relationship_to_victim_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/dataset/Perpetrators-Trend/ttus-3dym
    results = client.get("ttus-3dym")
    perpetrator_trends_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/dataset/Maltreatment-Types-of-Victims/8bce-qw8w
    results = client.get("8bce-qw8w")
    maltreatment_victims_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/dataset/Children-by-Disposition/usvm-fdmd
    results = client.get("usvm-fdmd")
    child_abuse_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/dataset/Children-Who-Received-an-Investigation-or-Alternat/7viv-bzwe
    results = client.get("7viv-bzwe")
    child_abuse_investigation_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/dataset/Child-Victims-by-Age/xn3e-yyaj
    results = client.get("xn3e-yyaj")
    child_abuse_by_age_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/dataset/Child-Victims-Trend/qwij-f3kq
    results = client.get("qwij-f3kq")
    child_abuse_by_trend_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/dataset/Child-Fatalities-by-Submission-Type/9c49-3jtk
    results = client.get("9c49-3jtk")
    child_fatalities_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/dataset/Child-Fatalities-Trend/u7xm-yva2
    results = client.get("u7xm-yva2")
    child_fatalities_trend_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/Hospital/HHS-IDs/vz64-k9wr
    results = client.get("vz64-k9wr")
    hhs_id_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/Health/COVID-19-Community-Vulnerability-Crosswalk-Crosswa/x2y5-9muu
    results = client.get("x2y5-9muu")
    community_vulnerability_crosswalk_census_tract_df = pd.DataFrame.from_records(results)

    # source: https://healthdata.gov/Health/COVID-19-Community-Vulnerability-Crosswalk-Rank-Or/rnkf-8dm6
    results = client.get("rnkf-8dm6")
    community_vulnerability_crosswalk_by_rank_order_score_df = pd.DataFrame.from_records(results)

    return {
        "child_protective_services.csv": child_protective_services_df,
        "relationship_to_victim.csv": relationship_to_victim_df,
        "perpetrator_trends.csv": perpetrator_trends_df,
        "maltreatment_victims.csv": maltreatment_victims_df,
        "child_abuse.csv": child_abuse_df,
        "child_abuse_investigation.csv": child_abuse_investigation_df,
        "child_abuse_by_age.csv": child_abuse_by_age_df,
        "child_abuse_by_trend.csv": child_abuse_by_trend_df,
        "child_fatalities.csv": child_fatalities_df,
        "child_fatalities_trend.csv": child_fatalities_trend_df,
        "hhs_id.csv": hhs_id_df,
        "community_vulnerability_crosswalk_census_tract.csv": community_vulnerability_crosswalk_census_tract_df,
        "community_vulnerability_crosswalk_by_rank_order_score.csv": community_vulnerability_crosswalk_by_rank_order_score_df
    }
    
    
if __name__ == '__main__':
    dicter = get_cdc()
    [dicter[key].to_csv(key, index=False) for key in dicter]
    dicter = healthdata_gov()
    [dicter[key].to_csv(key, index=False) for key in dicter]
    get_hrsa()
