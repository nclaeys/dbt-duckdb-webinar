FROM public.ecr.aws/dataminded/dbt:v1.5.0

WORKDIR /app
COPY . .

WORKDIR /app/dbt/coffee_shop_dbt

ENV DBT_PROFILES_DIR="/app/dbt"
ENV DBT_PROJECT_DIR="/app/dbt/coffee_shop_dbt"
ENV DBT_USE_COLORS="false"

# install dependencies
RUN dbt deps
