{% set models_to_generate = codegen.get_models(directory='dimension', prefix='dim_pe') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}