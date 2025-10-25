"""create cases table

Revision ID: 2430c050dd7f
Revises: 
Create Date: 2025-10-25 20:16:39.844479

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '2430c050dd7f'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'cases',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('court_name', sa.String(length=255), nullable=True),
        sa.Column('case_number', sa.String(length=100), nullable=True),
        sa.Column('case_proc', sa.String(length=100), nullable=True),
        sa.Column('registration_date', sa.Date(), nullable=True),
        sa.Column('judge', sa.String(length=255), nullable=True),
        sa.Column('judges', sa.Text(), nullable=True),
        sa.Column('participants', sa.Text(), nullable=True),
        sa.Column('stage_date', sa.Date(), nullable=True),
        sa.Column('stage_name', sa.String(length=255), nullable=True),
        sa.Column('cause_result', sa.Text(), nullable=True),
        sa.Column('cause_dep', sa.String(length=255), nullable=True),
        sa.Column('type', sa.String(length=255), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_cases_case_number', 'cases', ['case_number'], unique=False)
    op.create_unique_constraint('uq_cases_case_number', 'cases', ['case_number'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('uq_cases_case_number', 'cases', type_='unique')
    op.drop_index('ix_cases_case_number', table_name='cases')
    op.drop_table('cases')
