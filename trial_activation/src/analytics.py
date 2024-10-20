import pandas as pd
from sqlalchemy import create_engine, text

class TrialAnalytics:
    def __init__(self, db_path='sqlite:///trial_data.db'):
        self.engine = create_engine(db_path)

    def trial_activation_rate(self):
        with self.engine.connect() as conn:
            query = text('''
                SELECT 
                    (SELECT COUNT(*) FROM trial_activation) AS activated_orgs,
                    (SELECT COUNT(DISTINCT organization_id) FROM staging_behavioral_events) AS total_orgs
            ''')
            result = conn.execute(query).fetchone()
            activated_orgs, total_orgs = result

        rate = activated_orgs / total_orgs if total_orgs > 0 else 0
        return rate

    def time_to_activation(self):
        with self.engine.connect() as conn:
            query = text('''
                WITH activation_times AS (
                    SELECT 
                        organization_id,
                        MIN(timestamp) AS first_event,
                        MAX(timestamp) AS last_event
                    FROM staging_behavioral_events
                    GROUP BY organization_id
                ),
                activated_orgs AS (
                    SELECT organization_id FROM trial_activation
                )
                SELECT AVG(JULIANDAY(last_event) - JULIANDAY(first_event)) AS avg_days
                FROM activation_times
                WHERE organization_id IN (SELECT organization_id FROM activated_orgs)
            ''')
            result = conn.execute(query).fetchone()
            avg_days = result[0] if result[0] is not None else 0

        return avg_days

    def goal_completion_rates(self):
        with self.engine.connect() as conn:
            query = text('''
                SELECT 
                    SUM(goal_shift_created) AS shift_created,
                    SUM(goal_employee_invited) AS employee_invited,
                    SUM(goal_punched_in) AS punched_in,
                    SUM(goal_punch_in_approved) AS punch_in_approved,
                    SUM(goal_advanced_features) AS advanced_features,
                    COUNT(*) AS total_orgs
                FROM trial_goals
            ''')
            result = conn.execute(query).fetchone()

        total_orgs = result.total_orgs
        rates = {
            'Shift Created': result.shift_created / total_orgs,
            'Employee Invited': result.employee_invited / total_orgs,
            'Punched In': result.punched_in / total_orgs,
            'Punch In Approved': result.punch_in_approved / total_orgs,
            'Advanced Features': result.advanced_features / total_orgs
        }
        return rates

    def feature_engagement_rate(self):
        with self.engine.connect() as conn:
            query = text('''
                SELECT 
                    activity_detail,
                    COUNT(DISTINCT organization_id) AS engaged_orgs,
                    (SELECT COUNT(DISTINCT organization_id) FROM staging_behavioral_events) AS total_orgs
                FROM staging_behavioral_events
                WHERE activity_name = 'Page.Viewed' 
                  AND activity_detail IN ('Revenue', 'Integrations', 'Absence', 'Availability')
                GROUP BY activity_detail
            ''')
            results = conn.execute(query).fetchall()

        rates = {row.activity_detail: row.engaged_orgs / row.total_orgs for row in results}
        return rates

# Example usage
if __name__ == "__main__":
    analytics = TrialAnalytics()
    
    print(f"Trial Activation Rate: {analytics.trial_activation_rate():.2%}")
    print(f"Average Time to Activation: {analytics.time_to_activation():.2f} days")
    
    print("\nGoal Completion Rates:")
    for goal, rate in analytics.goal_completion_rates().items():
        print(f"  {goal}: {rate:.2%}")
    
    print("\nFeature Engagement Rates:")
    for feature, rate in analytics.feature_engagement_rate().items():
        print(f"  {feature}: {rate:.2%}")
