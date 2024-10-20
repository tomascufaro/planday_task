import pytest
import pandas as pd
from sqlalchemy import create_engine, text

@pytest.fixture
def db_connection():
    engine = create_engine('sqlite:///trial_data.db')
    with engine.connect() as conn:
        yield conn

def test_1nf_atomic_values(db_connection):
    atomic_check = db_connection.execute(text('''
        SELECT COUNT(*) as non_atomic_count
        FROM staging_behavioral_events
        WHERE activity_name LIKE '%;%' OR activity_detail LIKE '%;%'
    ''')).fetchone()
    
    assert atomic_check.non_atomic_count == 0, f"Found {atomic_check.non_atomic_count} non-atomic values"


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
    
    org_count = functional_dependencies.org_count
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
