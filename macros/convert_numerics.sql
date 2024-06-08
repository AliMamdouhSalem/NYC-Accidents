{% macro convert_numerics(number) -%}

    case 
        when {{number}} is null then 0
        else cast({{number}} as integer) 
    end

{%- endmacro %}