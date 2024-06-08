{% macro handle_ages(age) -%}
    case   
        when cast({{age}} as integer)<0 then -1
        when cast({{age}} as integer) is null then -1
        when cast({{age}} as integer)>125 then -1
        else cast({{age}} as integer)
    end 
{%- endmacro%}