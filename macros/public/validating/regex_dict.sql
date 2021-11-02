{#
#  This file contains significant part of code licensed under:
#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#}

{% macro get_regex_for(to_validate) %}

    {% set regexp_dict = {
        'number_whole': '^\-?[0-9]+$',
        'number_decimal_point': '^\-?[0-9]+\.[0-9]+$',
        'number_decimal_comma': '^\-?[0-9]+,[0-9]+$',
        'number_percentage': '^\-?[0-9]+([\.,][0-9]+)? ?%$',
        'number_percentage_point': '^\-?[0-9]+([\.][0-9]+)? ?%$',
        'number_percentage_comma': '^\-?[0-9]+([,][0-9]+)? ?%$',
        'date_eu': '^([1-9]|0[1-9]|[12][0-9]|3[01])[-\./]([1-9]|0[1-9]|1[012])[-\./](19|20)?[0-9][0-9]$',
        'date_us': '^([1-9]|0[1-9]|1[012])[-\./]([1-9]|0[1-9]|[12][0-9]|3[01])[-\./](19|20)?[0-9][0-9]$',
        'date_inverse': '^(19|20)[0-9][0-9][-\./]?([1-9]|0[1-9]|1[012])[-\./]?([1-9]|0[1-9]|[12][0-9]|3[01])$',
        'time_24h': '^([01][0-9]|2[0-3]):([0-5][0-9])$',
        'time_12h': '^(1[0-2]|0?[1-9]):[0-5][0-9]$',
        'time': '^([0-9]|1[0-9]|2[0-4])[:-]([0-9]|[0-5][0-9])([:-]([0-9]|[0-5][0-9])(,[0-9]+)?)?$',
        'date_iso_8601':
            '^'
            '([1-9][0-9]{3}-((0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-8])|(0[13-9]|1[0-2])-(29|30)|(0[13578]|1[02])-31)|'
            '([1-9][0-9](0[48]|[2468][048]|[13579][26])|([2468][048]|[13579][26])00)-02-29)'

            'T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]+)?'

            '(Z|[+-][01][0-9]:[0-5][0-9])?'
            '$',
        'uuid': '^[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}$',
        'ipv4_address': '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$',
        'ipv6_address': '^((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3}))|:)))(%.+)?$',
        'email': '^[A-Za-z0-9.-_%]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$',
        } %}

    {% set base_regex = regexp_dict[to_validate] %}
    {% set qualifed_regex = adapter.dispatch('get_regex_for', 're_data')(base_regex) %}
    {{ return(qualifed_regex) }}

{% endmacro %}

{% macro default__get_regex_for(pattern) %}
    {{ return (pattern) }}
{% endmacro %}

{% macro redshift__get_regex_for(pattern) %}
    {% set changed = modules.re.sub('\.', '\\.', pattern) %}
    {% set changed = modules.re.sub('\-', '\\-', changed) %}
    {{ return (changed) }}
{% endmacro %}

{% macro snowflake__get_regex_for(pattern) %}
    {% set changed = modules.re.sub('\.', '\\.', pattern) %}
    {% set changed = modules.re.sub('\-', '\\-', changed) %}
    {{ return (changed) }}
{% endmacro %}