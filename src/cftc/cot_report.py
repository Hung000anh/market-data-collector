from cot_reports import cot_year

def fetch_cftc_report(year: int):
        return cot_year(year, cot_report_type="legacy_fut", store_txt=False, verbose=False)