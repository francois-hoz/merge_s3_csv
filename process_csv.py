import pandas as pd
from math import floor
from time import localtime, strftime
import os

from generate_csv import OUTPUT_FILE



# --- CONFIGURATION ---
INPUT_FILE = "raw_aws_data.csv"
OUTPUT_FILE = "processed_attribution_data.csv"
# -----------------------------------------------------------------------------

def get_quarter(date_str):
    """ Convertit une date 'YYYY-MM-DD' en trimestre 'QX-YYYY'. """
    year, month, _ = map(int, date_str.split('-'))
    quarter = (month - 1) // 3 + 1
    return f"Q{quarter}-{year}"


def process_csv():

    if not os.path.isfile(INPUT_FILE):
        print(f"Le fichier {INPUT_FILE} n'existe pas.")
        return

    """Lit le CSV d'entrée, traite les données, et sauvegarde le résultat."""
    df = pd.read_csv(INPUT_FILE)


    df = df.sort_values('{created_at}')
    # Conversion des epochs en dates lisibles
    df['event_date']   = df['{created_at}'].apply(lambda x:strftime("%Y-%m-%d", localtime(x)))
    df['install_date'] = df['{installed_at}'].apply(lambda x:strftime("%Y-%m-%d", localtime(x)))

    # Identification de la première transaction par utilisateur
    first_payment_per_user = df.groupby('{random_user_id}').first()[['{created_at}', '{subscription_event_type}']]
    first_payment_per_user = first_payment_per_user.rename({
                                                '{created_at}':'first_payment_date', 
                                                '{event_name}':'first_payment_event_name'}, axis=1)
    
    # Ranking des événements par utilisateur et par date
    event_rank = df[['event_date','{random_user_id}','{event_name}', '{currency}']].groupby(['event_date','{random_user_id}','{event_name}']).rank(method='first')
    event_rank.rename(columns={'{currency}':'rank'}, inplace=True)
    df['rank'] = event_rank

    # Fusion des données pour inclure la date de première transaction
    revenue_data = df.merge(
        first_payment_per_user,
        how='left',
        left_on='{random_user_id}',
        right_index=True).sort_values(['{random_user_id}','{created_at}'])
    
    # Calcul du temps écoulé depuis la première transaction
    revenue_data['time_since_first_payment'] = revenue_data['{created_at}'] - revenue_data['first_payment_date']

    # Extraction de l'année et du trimestre de l'événement
    revenue_data['event_date_year'] = revenue_data['event_date'].apply(lambda x:int(x[:4]))
    revenue_data['event_date_quarter'] = revenue_data['event_date'].apply(get_quarter)

    # Calcul du i-ème mois depuis la première transaction
    revenue_data['i_th_month_since_first_payment'] = revenue_data['time_since_first_payment'] /(30*24*3600)
    revenue_data['i_th_month_since_first_payment'] = revenue_data['i_th_month_since_first_payment'].apply(lambda x:1+floor(x)).astype(int)

    # Calcul du revenu net (après commission Adjust)
    revenue_data['net_revenue'] = revenue_data['{reporting_revenue}'] * 0.7

    # Définition des ratios de revenu selon les campagnes et conditions

    # Initialisation à 0
    revenue_data['revenue_deal_ratio'] = 0.0

    # Pour Hozana et Hozana_affilie, sauf certains adgroups, 50% pour les 12 premiers mois
    revenue_data.loc[(revenue_data['{campaign_name}']=='hozana') & \
                 (~revenue_data['{adgroup_name}'].isin(['cp-semaine-sainte', 'CP', 'community'])) & \
                 (revenue_data['i_th_month_since_first_payment'] <= 12), 'revenue_deal_ratio'] = 0.5

    revenue_data.loc[(revenue_data['{campaign_name}']=='Hozana') & \
                 (~revenue_data['{adgroup_name}'].isin(['CP-rentree', 'CP', 'cdi-mdj'])) & \
                 (revenue_data['i_th_month_since_first_payment'] <= 12), 'revenue_deal_ratio'] = 0.5

    revenue_data.loc[(revenue_data['{campaign_name}']=='Hozana_affilie') & \
                 (~revenue_data['{adgroup_name}'].isin(['cp-semaine-sainte', 'CP', 'community'])) & \
                 (revenue_data['i_th_month_since_first_payment'] <= 12), 'revenue_deal_ratio'] = 0.5

    # Pour Icnews, 50% pour les 12 premiers mois, 25% pour les 13-36 mois
    revenue_data.loc[(revenue_data['{campaign_name}']=='Icnews') & \
                 (revenue_data['i_th_month_since_first_payment'] <= 12), 'revenue_deal_ratio'] = 0.5

    revenue_data.loc[(revenue_data['{campaign_name}']=='Icnews') & \
                 (revenue_data['i_th_month_since_first_payment'] >= 13) & \
                 (revenue_data['i_th_month_since_first_payment'] <= 36), 'revenue_deal_ratio'] = 0.25

    # Calcul du revenu net après application du deal
    revenue_data['net_revenue_deal_amount'] = revenue_data['net_revenue'] * revenue_data['revenue_deal_ratio']

    # Sauvegarde du CSV traité
    revenue_data.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    process_csv()