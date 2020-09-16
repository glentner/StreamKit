/* This program is free software: you can redistribute it and/or modify it under the
 * terms of the Apache License (v2.0) as published by the Apache Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the Apache License for more details.
 *
 * You should have received a copy of the Apache License along with this program.
 * If not, see <https://www.apache.org/licenses/LICENSE-2.0>.
 */

SELECT create_hypertable('{{ SCHEMA }}.message', 'time', chunk_time_interval => interval '1 day');
