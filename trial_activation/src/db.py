import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Create a database connection
engine = create_engine('sqlite:///trial_data.db')

# Load the data into a DataFrame and set appropriate data types
data = pd.read_csv('trial_activation/data/analytics_engineering_task.csv')

# Convert timestamp to datetime
data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])

# Ensure organization_id is string (for UUID)
data['ORGANIZATION_ID'] = data['ORGANIZATION_ID'].astype(str)
print('Number of unique organizations in source data:', len(data.ORGANIZATION_ID.unique()))
# Load the data into the database
data.to_sql('behavioral_events', con=engine, if_exists='replace', index=False)

# Step 1: Staging Layer - Create staging table from behavioral_events
with engine.connect() as conn:
    # Drop the existing table if it exists
    conn.execute(text('DROP TABLE IF EXISTS staging_behavioral_events'))
    
    # Create the staging_behavioral_events table
    conn.execute(text('''
        CREATE TABLE staging_behavioral_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id TEXT NOT NULL,
            activity_name TEXT,
            activity_detail TEXT,
            timestamp DATETIME NOT NULL
        )
    '''))
    
    # Prepare and execute the INSERT statement
    insert_query = text('''
        INSERT INTO staging_behavioral_events (organization_id, activity_name, activity_detail, timestamp)
        SELECT organization_id, activity_name, activity_detail, timestamp
        FROM behavioral_events
    ''')
    
    conn.execute(insert_query)
    
    # Commit the transaction
    conn.commit()
    # Check if any data was inserted
    count_behavioral_events = conn.execute(text('SELECT COUNT(*) FROM behavioral_events')).scalar()
    count_staging_behavioral_events = conn.execute(text('SELECT COUNT(*) FROM staging_behavioral_events')).scalar()
    organizations_staging_behavioral_events = conn.execute(text('SELECT COUNT(DISTINCT organization_id) FROM staging_behavioral_events')).scalar()
    print(f"Total rows in staging_behavioral_events: {count_staging_behavioral_events}")
    print(f'total rows in behavioral_events: {count_behavioral_events}')
    print(f'Number of unique organizations in staging_behavioral_events: {organizations_staging_behavioral_events}')
    
    

# Step 2: Trial Goals Mart
with engine.connect() as conn:
    # Drop the existing table if it exists
    conn.execute(text('DROP TABLE IF EXISTS trial_goals'))
    
    # Create the trial_goals table
    conn.execute(text('''
        CREATE TABLE trial_goals (
            organization_id TEXT PRIMARY KEY,
            goal_shift_created BOOLEAN NOT NULL,
            goal_employee_invited BOOLEAN NOT NULL,
            goal_punched_in BOOLEAN NOT NULL,
            goal_punch_in_approved BOOLEAN NOT NULL,
            goal_advanced_features BOOLEAN NOT NULL
        )
    '''))
    
    # Prepare and execute the INSERT statement
    insert_query = text('''
        INSERT INTO trial_goals (
            organization_id,
            goal_shift_created,
            goal_employee_invited,
            goal_punched_in,
            goal_punch_in_approved,
            goal_advanced_features
        )
        SELECT
            organization_id,
            CASE WHEN SUM(CASE WHEN activity_name = 'Shift.Created' THEN 1 ELSE 0 END) >= 2 THEN 1 ELSE 0 END,
            CASE WHEN SUM(CASE WHEN activity_name = 'Hr.Employee.Invited' THEN 1 ELSE 0 END) >= 1 THEN 1 ELSE 0 END,
            CASE WHEN SUM(CASE WHEN activity_name = 'PunchClock.PunchedIn' THEN 1 ELSE 0 END) >= 1 THEN 1 ELSE 0 END,
            CASE WHEN SUM(CASE WHEN activity_name = 'PunchClock.Approvals.EntryApproved' THEN 1 ELSE 0 END) >= 1 THEN 1 ELSE 0 END,
            CASE WHEN SUM(CASE WHEN activity_name = 'Page.Viewed' AND activity_detail IN ('revenue', 'integrations', 'absence', 'availability') THEN 1 ELSE 0 END) >= 2 THEN 1 ELSE 0 END
        FROM staging_behavioral_events
        GROUP BY organization_id
    ''')
    
    conn.execute(insert_query)
    
    # Commit the transaction
    conn.commit()

    # Verify the data insertion
    result = conn.execute(text('SELECT COUNT(*) FROM trial_goals')).scalar()
    print(f"Number of rows inserted into trial_goals: {result}")


# After creating trial_goals table
with engine.connect() as conn:
    # Data Consistency Check
    consistency_check = conn.execute(text('''
        SELECT 
            COUNT(DISTINCT se.organization_id) as staging_org_count,
            COUNT(DISTINCT tg.organization_id) as trial_goals_org_count,
        FROM staging_behavioral_events se
        LEFT JOIN trial_goals tg ON se.organization_id = tg.organization_id
    ''')).fetchone()
    
    print("\nConsistency Check:")
    print(f"organizations in behavioral_events: {len(data.ORGANIZATION_ID.unique())}")
    print(f"Organizations in Staging: {consistency_check.staging_org_count}")
    print(f"Organizations in Trial Goals: {consistency_check.trial_goals_org_count}")

# Step 3: Trial Activation Mart
with engine.connect() as conn:
    # Drop the existing table if it exists
    conn.execute(text('DROP TABLE IF EXISTS trial_activation'))
    
    # Create the trial_activation table
    conn.execute(text('''
        CREATE TABLE trial_activation (
            organization_id TEXT PRIMARY KEY
        )
    '''))
    
    # Prepare and execute the INSERT statement
    insert_query = text('''
        INSERT INTO trial_activation (organization_id)
        SELECT organization_id
        FROM trial_goals
        WHERE goal_shift_created = 1
          AND goal_employee_invited = 1
          AND goal_punched_in = 1
          AND goal_punch_in_approved = 1
          AND goal_advanced_features = 1
    ''')
    
    conn.execute(insert_query)
    
    # Commit the transaction
    conn.commit()

    # Verify the data insertion
    result = conn.execute(text('SELECT COUNT(*) FROM trial_activation')).scalar()
    print(f"Number of rows inserted into trial_activation: {result}")

# Explanation of Layers
# - Staging Layer: Here, we loaded the raw data into a staging table without any transformations.
# - Integration Layer: The 'trial_goals' table aggregates the event data to track whether each trial goal was completed by an organization.
# - Data Mart Layer: The 'trial_activation' table holds information about organizations that have achieved full activation by completing all trial goals.
