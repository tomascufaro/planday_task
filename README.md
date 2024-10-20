# Trial Activation Analysis

This project analyzes trial activation data for a SaaS product, creating a database structure and providing analytics functionality.

## Database Creation and Verification

The database is created in several steps, each building on the previous one. At each step, we perform verifications to ensure data integrity and consistency.

1. **Data Loading**
   - CSV data is loaded into a temporary 'behavioral_events' table.

2. **Staging Layer**
   - A 'staging_behavioral_events' table is created with the following structure:
     ```sql
     CREATE TABLE staging_behavioral_events (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         organization_id INTEGER NOT NULL,
         activity_name TEXT NOT NULL,
         activity_detail TEXT,
         timestamp DATETIME NOT NULL
     )
     ```
   - Data is cleaned and transferred from 'behavioral_events' to 'staging_behavioral_events'.
   - **Verification**: After loading, we perform the following checks:
     - Total number of records
     - Number of unique organizations
     - Count of null values in critical fields (organization_id, activity_name, timestamp)
     - Timestamp range (min and max)

3. **Trial Goals Mart**
   - A 'trial_goals' table is created to track goal completion for each organization:
     ```sql
     CREATE TABLE trial_goals (
         organization_id INTEGER PRIMARY KEY,
         goal_shift_created BOOLEAN NOT NULL,
         goal_employee_invited BOOLEAN NOT NULL,
         goal_punched_in BOOLEAN NOT NULL,
         goal_punch_in_approved BOOLEAN NOT NULL,
         goal_advanced_features BOOLEAN NOT NULL,
         FOREIGN KEY (organization_id) REFERENCES staging_behavioral_events(organization_id)
     )
     ```
   - Data is aggregated from 'staging_behavioral_events' to populate 'trial_goals'.
   - **Verification**: After creation, we perform a consistency check:
     - Compare the number of unique organizations in 'staging_behavioral_events' and 'trial_goals'

4. **Trial Activation Mart**
   - A 'trial_activation' table is created to identify fully activated trials:
     ```sql
     CREATE TABLE trial_activation (
         organization_id INTEGER PRIMARY KEY,
         FOREIGN KEY (organization_id) REFERENCES trial_goals(organization_id)
     )
     ```
   - Organizations that have completed all goals are inserted into this table.

5. **Final Verification**
   - After all tables are created, we perform a final verification:
     - Output the contents of 'trial_goals' and 'trial_activation' tables for manual inspection

These verification steps ensure that:
- All data is correctly loaded from the source CSV
- No critical data is missing
- The data falls within expected date ranges
- All organizations are correctly transferred between tables
- The final activation status is correctly calculated

By performing these checks at each stage, we maintain data integrity throughout the ETL process and ensure the reliability of our analytics results.

## Analytics Module

The analytics module (`TrialAnalytics` class in `analytics.py`) provides methods to analyze the trial activation data:

### `trial_activation_rate()`
- Calculates the overall trial activation rate.
- Returns the percentage of organizations that have completed all activation goals.
- Formula: (number of activated organizations) / (total number of organizations)

### `time_to_activation()`
- Calculates the average time it takes for organizations to complete all activation goals.
- Returns the average time in days.
- Only considers organizations that have successfully activated.

### `goal_completion_rates()`
- Calculates the completion rate for each individual goal.
- Returns a dictionary with goal names as keys and completion rates as values.
- Goals include: Shift Created, Employee Invited, Punched In, Punch In Approved, and Advanced Features.

### `feature_engagement_rate()`
- Analyzes the engagement rate for specific features.
- Returns a dictionary with feature names as keys and engagement rates as values.
- Features analyzed: Revenue, Integrations, Absence, and Availability.
- Engagement is measured by the 'Page.Viewed' activity for each feature.

## Usage

To use this module:

1. Ensure the database is set up correctly using the provided SQL scripts.
2. Import the TrialAnalytics class from the analytics module in your Python script.
3. Create an instance of the TrialAnalytics class.
4. Call the desired methods to analyze your trial activation data.

Example:
