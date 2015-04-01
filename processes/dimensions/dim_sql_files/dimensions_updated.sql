select
  distinct(name)
from dwao.dimensions_updated
  where
    name in %(dims)s
    and dateend between timestamp(%(dateend)s, '00:00:00') and timestamp(%(dateend)s, '23:59:59')
    and status = %(status)s