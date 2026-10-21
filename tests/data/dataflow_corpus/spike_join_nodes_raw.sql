select n.externalId, r.x from cdf_nodes('sp','V','1') n join db.t r on n.externalId=r.id
