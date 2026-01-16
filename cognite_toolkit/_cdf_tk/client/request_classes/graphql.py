UPSERT_BODY = """
mutation UpsertGraphQlDmlVersion($dmCreate: GraphQlDmlVersionUpsert!) {
    upsertGraphQlDmlVersion(graphQlDmlVersion: $dmCreate) {
        errors {
            kind
            message
            hint
            location {
                start {
                    line
                    column
                }
            }
        }
        result {
            space
            externalId
            version
            name
            description
            graphQlDml
            isGlobal
            createdTime
            lastUpdatedTime
        }
    }
}
"""
