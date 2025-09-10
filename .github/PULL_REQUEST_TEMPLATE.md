<!--
Please comply with the contributing guidelines and best practices of this repository.
Pull Request titles must follow conventional commits format: type(scope): description
Example: feat(correlation): add canonical ID fuzzy matching
Example: fix(ingestion): resolve OpenLineage event parsing error
-->

## Description
<!-- Provide a detailed description of the changes in this PR -->

## Issue Link
<!-- Link to the related issue (if applicable) -->

## Type of Change
<!-- Mark the appropriate option with an 'x' -->
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Code refactoring
- [ ] Database schema change (migration required)
- [ ] Plugin implementation (dbt, Airflow, Great Expectations)
- [ ] Performance optimization
- [ ] CI/CD or workflow changes
- [ ] Other (please describe):

## Quality Assurance Checklist
<!-- Mark completed items with an 'x' -->
- [ ] I have followed the branch naming convention (`type/description`)
- [ ] I have followed conventional commits format for all commit messages
- [ ] I have reviewed my own code before requesting review
- [ ] I have added/updated comprehensive tests for the changes
- [ ] All tests pass locally (`make test`)
- [ ] Code passes linting checks (`make lint`)
- [ ] Code passes formatting checks (`make fmt`)
- [ ] Code passes Go vet checks (`make vet`)
- [ ] I have updated documentation where necessary (README.md, docstrings, etc.)
- [ ] I have verified my changes work with Docker (`make docker-dev-setup && make docker-dev`)

## Correlator-Specific Checklist
<!-- Mark completed items with an 'x' if applicable -->
- [ ] Database migrations are tested and include proper rollback (`migrations/*.down.sql`)
- [ ] Changes maintain >90% correlation accuracy (validated with sample data)
- [ ] OpenLineage compliance verified for ingestion changes
- [ ] Canonical ID format validation passes (`system:id` pattern)
- [ ] Performance impact assessed for correlation workloads
- [ ] Plugin integration tested (if applicable)
- [ ] Zero-config Docker deployment still works (`docker-compose up`)
- [ ] Health checks pass (`make docker-health`)

## Performance Impact
<!-- If applicable, describe the performance implications -->
- [ ] No performance impact expected
- [ ] Performance improvement (describe the improvement)
- [ ] Performance regression risk (describe mitigation)
- [ ] Correlation latency impact assessed (<5 minute target maintained)

## Migration Notes
<!-- For database schema changes -->
- [ ] No database changes
- [ ] New migration files included with proper rollback
- [ ] Migration tested locally with Docker setup
- [ ] Migration maintains data integrity and constraints

## Additional Notes
<!-- Any additional information that reviewers should know -->

## Screenshots/Logs
<!-- If applicable, add screenshots, log outputs, or performance metrics -->

## Checklist for Reviewers
<!-- For the PR reviewer -->
- [ ] Code follows Correlator project conventions and Go standards
- [ ] Tests cover the changes appropriately (including correlation-specific tests)
- [ ] Documentation is clear and sufficient
- [ ] Database changes are properly migrated and reversible
- [ ] Performance implications are acceptable for correlation workloads
- [ ] OpenLineage compliance maintained (if applicable)
- [ ] Zero-config Docker deployment verified