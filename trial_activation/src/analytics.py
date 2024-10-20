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

    def goal_achievement_probability(self, goal, days):
        with self.engine.connect() as conn:
            query = text('''
                WITH goal_achievements AS (
                    SELECT organization_id,
                           CASE 
                               WHEN :goal = 'goal_shift_created' THEN goal_shift_created
                               WHEN :goal = 'goal_employee_invited' THEN goal_employee_invited
                               WHEN :goal = 'goal_punched_in' THEN goal_punched_in
                               WHEN :goal = 'goal_punch_in_approved' THEN goal_punch_in_approved
                               WHEN :goal = 'goal_advanced_features' THEN goal_advanced_features
                               ELSE 0
                           END AS achieved
                    FROM trial_goals
                )
                SELECT AVG(CASE WHEN se.time_since_first_activity <= :days * 86400
                                AND ga.achieved = 1 THEN 1.0 ELSE 0.0 END) AS probability
                FROM staging_behavioral_events se
                JOIN goal_achievements ga ON se.organization_id = ga.organization_id
                WHERE se.timestamp = se.first_activity_timestamp
            ''')
            result = conn.execute(query, {'goal': goal, 'days': days}).fetchone()
            probability = result[0] if result[0] is not None else 0

        return probability

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

    print("\nGoal Achievement Probabilities (within 30 days):")
    goals = ['goal_shift_created', 'goal_employee_invited', 'goal_punched_in', 
             'goal_punch_in_approved', 'goal_advanced_features']
    for goal in goals:
        prob = analytics.goal_achievement_probability(goal, 120)
        print(f"  {goal}: {prob:.2%}")
