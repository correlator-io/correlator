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
- [ ] I have reviewed my own code before requesting review
- [ ] All tests pass locally (`make test`)
- [ ] Code passes go vet checks (`make vet`)
- [ ] Code passes go lint checks (`make fmt`)
- [ ] Code passes go lint checks (`make lint`)
- [ ] I have added/updated tests for the changes
- [ ] Database migrations include proper rollback (if applicable)
- [ ] Zero-config Docker deployment still works (`docker-compose up`)
- [ ] OpenLineage compliance verified (for ingestion changes)
- [ ] Performance impact assessed (<5 minute correlation target maintained)
- [ ] Documentation updated where necessary
- [ ] Changes follow conventional commits format

## Additional Notes
<!-- Any additional information that reviewers should know -->