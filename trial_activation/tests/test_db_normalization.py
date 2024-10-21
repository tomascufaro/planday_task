import pytest
import pandas as pd
from sqlalchemy import create_engine, text
import os

@pytest.fixture
def db_connection():
    # Get the path to the trial_activation director
    
    engine = create_engine(f'sqlite:///trial_activation/trial_data.db')
    with engine.connect() as conn:
        yield conn

# Test 1NF: Atomic values - no semicolons in activity_name or activity_detail
def test_1nf_atomic_values(db_connection):
    atomic_check = db_connection.execute(text('''
        SELECT COUNT(*) as non_atomic_count
        FROM staging_behavioral_events
        WHERE activity_name LIKE '%;%' OR activity_detail LIKE '%;%'
    ''')).fetchone()
    
    assert atomic_check.non_atomic_count == 0, f"Found {atomic_check.non_atomic_count} non-atomic values"


# Test 2NF: Functional dependencies - no more than 2 distinct values for each column in trial_goals
def test_2nf_3nf_functional_dependencies(db_connection):
    functional_dependencies = db_connection.execute(text('''
        SELECT 
            COUNT(DISTINCT organization_id) as org_count,
            COUNT(DISTINCT goal_shift_created) as shift_created_count,
            COUNT(DISTINCT goal_employee_invited) as employee_invited_count,
            COUNT(DISTINCT goal_punched_in) as punched_in_count, 
            COUNT(DISTINCT goal_punch_in_approved) as punch_in_approved_count,
            COUNT(DISTINCT goal_advanced_features) as advanced_features_count
        FROM trial_goals
    ''')).fetchone()
    
    for column, count in functional_dependencies._asdict().items():
        if column != 'org_count':
            assert count <= 2, f"Unexpected number of distinct values in {column}: {count}"


def test_tables_not_empty(db_connection):
    # Get all table names
    tables = db_connection.execute(text("""
        SELECT name FROM sqlite_master WHERE type='table'
    """)).fetchall()
    
    for table in tables:
        table_name = table[0]
        
        # Check if table is not empty
        row_count = db_connection.execute(text(f"""
            SELECT COUNT(*) as count FROM {table_name}
        """)).fetchone().count
        
        assert row_count > 0, f"Table {table_name} is empty"
        print(f"Table {table_name} has {row_count} rows")


def test_organization_id_consistency_across_main_tables(db_connection):
    org_counts = db_connection.execute(text('''
        SELECT 
            (SELECT COUNT(DISTINCT organization_id) FROM behavioral_events) as be_count,
            (SELECT COUNT(DISTINCT organization_id) FROM staging_behavioral_events) as sbe_count,
            (SELECT COUNT(DISTINCT organization_id) FROM trial_goals) as tg_count
    ''')).fetchone()
    
    assert org_counts.be_count == org_counts.sbe_count == org_counts.tg_count, (
        f"Inconsistent organization_id counts: "
        f"behavioral_events: {org_counts.be_count}, "
        f"staging_behavioral_events: {org_counts.sbe_count}, "
        f"trial_goals: {org_counts.tg_count}"
    )

    # Additional check for exact match of organization_ids
    org_id_match = db_connection.execute(text('''
        SELECT COUNT(*) as match_count
        FROM (
            SELECT organization_id FROM behavioral_events
            INTERSECT
            SELECT organization_id FROM staging_behavioral_events
            INTERSECT
            SELECT organization_id FROM trial_goals
        )
    ''')).fetchone()

    assert org_id_match.match_count == org_counts.be_count, (
        f"Not all organization_ids match across tables. "
        f"Matching count: {org_id_match.match_count}, "
        f"Expected count: {org_counts.be_count}"
    )

    print(f"All main tables have {org_counts.be_count} unique organization_ids")



def test_goal_consistency_between_staging_and_trial_goals(db_connection):
    # Query to get goal counts from staging_behavioral_events
    staging_counts = db_connection.execute(text('''
        SELECT
            SUM(CASE WHEN activity_name = 'Shift.Created' THEN 1 ELSE 0 END) >= 2 as shift_created,
            SUM(CASE WHEN activity_name = 'Hr.Employee.Invited' THEN 1 ELSE 0 END) >= 1 as employee_invited,
            SUM(CASE WHEN activity_name = 'PunchClock.PunchedIn' THEN 1 ELSE 0 END) >= 1 as punched_in,
            SUM(CASE WHEN activity_name = 'PunchClock.Approvals.EntryApproved' THEN 1 ELSE 0 END) >= 1 as punch_in_approved,
            SUM(CASE WHEN activity_name = 'Page.Viewed' AND activity_detail IN ('revenue', 'integrations-overview', 'absence-accounts', 'availability') THEN 1 ELSE 0 END) >= 2 as advanced_features
        FROM staging_behavioral_events
        GROUP BY organization_id
    ''')).fetchall()

    # Query to get goal counts from trial_goals
    trial_goals_counts = db_connection.execute(text('''
        SELECT
            SUM(goal_shift_created) as shift_created,
            SUM(goal_employee_invited) as employee_invited,
            SUM(goal_punched_in) as punched_in,
            SUM(goal_punch_in_approved) as punch_in_approved,
            SUM(goal_advanced_features) as advanced_features
        FROM trial_goals
    ''')).fetchone()

    # Calculate totals from staging_behavioral_events
    staging_totals = {
        'shift_created': sum(row[0] for row in staging_counts),
        'employee_invited': sum(row[1] for row in staging_counts),
        'punched_in': sum(row[2] for row in staging_counts),
        'punch_in_approved': sum(row[3] for row in staging_counts),
        'advanced_features': sum(row[4] for row in staging_counts)
    }

    # Compare totals
    for goal, staging_total in staging_totals.items():
        trial_goal_total = getattr(trial_goals_counts, goal)
        assert staging_total == trial_goal_total, f"Mismatch in {goal}: staging={staging_total}, trial_goals={trial_goal_total}"

    print("All goal counts match between staging_behavioral_events and trial_goals")



