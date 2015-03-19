select
  distinct(name)
from dwao.dimensions_updated
  where
    name in (%(dims)s)
    and dateend between %(dateend)s and date_add(%(dateend)s, interval 1 day)
    and status = %(status)s