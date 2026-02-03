import pandas as pd
import numpy as np
from datetime import datetime

# Step 1: Load the updated purchases dataset
df = pd.read_excel('D24Customers_filtered_from_2023.xlsx')  # Make sure this includes 'Produkt-Gruppe'


# Step 2: Basic Cleaning
df['E-Mail'] = df['E-Mail'].str.strip().str.lower()
df['Datum'] = pd.to_datetime(df['Datum'], dayfirst=True)
df = df.dropna(subset=['E-Mail', 'Datum', 'Erste Zahlung'])

# Step 3: OUTLIER Removal (Monetary)
payment_threshold = df['Erste Zahlung'].quantile(0.99)
df = df[df['Erste Zahlung'] <= payment_threshold]

# Step 3.5: Purchase Journey Insights

# Sort data for sequential product tracking
df_sorted = df.sort_values(['E-Mail', 'Datum'])

# First product bought
first_products = df_sorted.groupby('E-Mail').first().reset_index()[['E-Mail', 'Datum', 'Produktname']]
first_products.rename(columns={'Datum': 'First_Purchase_Date', 'Produktname': 'First_Product'}, inplace=True)

# Last purchase date
last_dates = df_sorted.groupby('E-Mail')['Datum'].max().reset_index().rename(columns={'Datum': 'Last_Purchase_Date'})

# Sequence of all products bought (in order)
product_sequence = df_sorted.groupby('E-Mail')['product_type'].apply(
    lambda x: ' > '.join(x.fillna('').astype(str))
).reset_index()

product_sequence.rename(columns={'product_type': 'Product_Sequence'}, inplace=True)

# Akademie detection via 'Produkt-Gruppe'
akademie_keywords = ['Akademie1990', 'AKADEMIE', 'Akademie 3750']
df['is_Akademie'] = df['Produkt-Gruppe'].isin(akademie_keywords)

# Flag if customer ever bought an Akademie product
akademie_flag = df.groupby('E-Mail')['is_Akademie'].any().astype(int).reset_index().rename(columns={'is_Akademie': 'Bought_Akademie'})

# Merge all journey features
journey_info = first_products.merge(last_dates, on='E-Mail', how='left') \
                             .merge(product_sequence, on='E-Mail', how='left') \
                             .merge(akademie_flag, on='E-Mail', how='left')

# Step 4: Prepare RFM
snapshot_date = df['Datum'].max() + pd.Timedelta(days=1)

rfm = df.groupby('E-Mail').agg({
    'Datum': lambda x: (snapshot_date - x.max()).days,  # Recency
    'Bestell-ID': 'nunique',                             # Frequency
    'Erste Zahlung': 'sum'                               # Monetary
}).reset_index()

rfm.columns = ['E-Mail', 'Recency', 'Frequency', 'Monetary']

# Step 5: RFM Scoring
# Step 5: RFM Scoring
rfm['R_Score'] = pd.qcut(rfm['Recency'], 4, labels=[4, 3, 2, 1])
rfm['F_Score'] = pd.qcut(rfm['Frequency'].rank(method='first'), 4, labels=[1, 2, 3, 4])

# Handle potential duplicates in qcut for Monetary
monetary_series = rfm['Monetary']
bins = pd.qcut(monetary_series, q=4, retbins=True, duplicates='drop')[1]
num_bins = len(bins) - 1
labels = list(range(1, num_bins + 1))

rfm['M_Score'] = pd.qcut(monetary_series, q=num_bins, labels=labels, duplicates='drop')


rfm['RFM_Score'] = rfm['R_Score'].astype(str) + rfm['F_Score'].astype(str) + rfm['M_Score'].astype(str)

# Step 5.5: Add RFM segment names
def rfm_segment(row):
    recency = row['Recency']
    frequency = row['Frequency']
    monetary = row['Monetary']
    r = int(row['R_Score'])
    f = int(row['F_Score'])
    m = int(row['M_Score'])

    # 🆕 New Customers: 1 purchase only, very recent
    if recency <= 71 and frequency == 1:
        return 'New Customers'

    # 🏆 Champions: frequency > 4, top recency & monetary
    if frequency > 4 and r == 4 and f == 4 and m >= 3:
        return 'Champions'

    # 💚 Loyal Customers: frequency > 4, good recency & monetary
    elif frequency >= 4 and r >= 3 and f >= 3 and m >= 2:
        return 'Loyal Customers'

    # 🔄 Potential Loyalist: frequency ≥ 2, decent recency & monetary
    elif frequency >= 2 and r >= 2 and f >= 2 and m >= 2:
        return 'Potential Loyalist'

    # 👀 Needs Attention: frequency ≥ 1, but recency is getting low
    elif frequency >= 1 and r <= 2 and f >= 2 and m >= 2:
        return 'Needs Attention'

    # ⚠️ At Risk: poor recency, frequency and monetary
    elif r == 1 and f <= 2 and m <= 2:
        return 'At Risk'

    # ❓ Others
    else:
        return 'Others'

rfm['RFM_Segment'] = rfm.apply(rfm_segment, axis=1)

# Step 6: Simple Churn Prediction
rfm['Churn_Flag'] = np.where(rfm['Recency'] > 90, 1, 0)

# Step 7: Improved CLV Calculation

# 1. Average Order Value per customer
df['Order_Value'] = df['Erste Zahlung']
aov = df.groupby('E-Mail')['Order_Value'].mean().rename('AOV')

# 2. Purchase Frequency per customer
purchase_frequency = df.groupby('E-Mail')['Bestell-ID'].nunique().rename('Frequency_Count')

# 3. Merge into RFM
rfm = rfm.merge(aov, on='E-Mail', how='left')
rfm = rfm.merge(purchase_frequency, on='E-Mail', how='left')

# 4. Expected Customer Lifespan (based on churn)
rfm['Expected_Lifespan_Years'] = np.where(rfm['Churn_Flag'] == 0, 1.5, 0.5)

# 5. Final CLV Calculation
rfm['CLV'] = rfm['AOV'] * rfm['Frequency_Count'] * rfm['Expected_Lifespan_Years']

# Step 8: Merge Journey Info
rfm = rfm.merge(journey_info, on='E-Mail', how='left')
# Step 8.5: Add Name, Vorname, and Land
customer_info = df.sort_values('Datum').groupby('E-Mail').first().reset_index()[['E-Mail', 'Nachname', 'Vorname', 'Land', 'Zahlungsstatus']]
rfm = rfm.merge(customer_info, on='E-Mail', how='left')

# Step 9: Save to Excel
rfm.to_excel('rfm_clv_churn_output.xlsx', index=False)

print("✅ Done! File saved as 'rfm_clv_churn_journey_output.xlsx'.")
