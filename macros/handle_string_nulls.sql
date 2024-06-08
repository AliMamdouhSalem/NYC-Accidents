{% macro handle_string_nulls(field) -%}

    case 
        when {{field}} is null then 'NA'
        else {{field}}
    end

{%- endmacro %}