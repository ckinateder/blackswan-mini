import simfin as sf
import util

def 

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    sf.set_api_key(api_key=util.get_env_var("SIMFIN_KEY_ID"))
    sf.set_data_dir("~/simfin_data/")

    df_balance = sf.load_balance(variant="annual", market="us")
    df_income = sf.load_income(variant="annual", market="us")
    df_cashflow = sf.load_cashflow(variant="annual", market="us")
