let
    // === API Configuration ===
    apiUrl = "",
    accessToken = "",

    // === GraphQL Query ===
    baseQuery = "
    query ($cursor: String) {
      products(first: 100, after: $cursor) {
        edges {
          node {
            id
            title
            status
            productType
            vendor
            tags
            createdAt
            updatedAt
            publishedAt

            variants(first: 100) {
              edges {
                node {
                  id
                  title
                  sku
                  barcode
                  price
                  compareAtPrice
                  inventoryQuantity

                  inventoryItem {
                    id
                    unitCost {
                      amount
                      currencyCode
                    }
                  }
                }
              }
            }
          }
        }

        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }",

    // === Recursive function ===
    GetProducts = (cursor as nullable text, collected as list) =>
    let
        vars = if cursor <> null then [ cursor = cursor ] else [ cursor = null ],
        body = Json.FromValue([ query = baseQuery, variables = vars ]),

        response = Web.Contents(
            apiUrl,
            [
                Headers = [
                    #"Content-Type" = "application/json",
                    #"X-Shopify-Access-Token" = accessToken
                ],
                Content = body
            ]
        ),

        json = Json.Document(response),
        edges = json[data][products][edges],
        pageInfo = json[data][products][pageInfo],
        hasNext = pageInfo[hasNextPage],
        nextCursor = pageInfo[endCursor],

        // === Build rows ===
        currentPage =
            List.Combine(
                List.Transform(edges, each
                    let
                        product = _[node],

                        baseInfo = [
                            product_id = product[id],
                            product_title = product[title],
                            product_status = product[status],
                            product_type = product[productType],
                            vendor = product[vendor],
                            tags = Text.Combine(product[tags], ", "),
                            createdAt = product[createdAt],
                            updatedAt = product[updatedAt],
                            publishedAt = product[publishedAt]
                        ],

                        variants =
                            List.Transform(product[variants][edges], each
                                let
                                    v = _[node],
                                    costData = v[inventoryItem][unitCost]
                                in
                                    baseInfo & [
                                        variant_id = v[id],
                                        variant_title = v[title],
                                        sku = v[sku],
                                        barcode = v[barcode],

                                        // === KEEPING COMMA AS TEXT ===
                                        price = Text.Replace(Text.From(v[price]), ".", ","),
                                        compareAtPrice = Text.Replace(Text.From(v[compareAtPrice]), ".", ","),
                                        cost_amount = if costData <> null then Text.Replace(Text.From(costData[amount]), ".", ",") else null,

                                        cost_currency = if costData <> null then costData[currencyCode] else null,
                                        inventoryQuantity = v[inventoryQuantity]
                                    ]
                            )
                    in
                        variants
                )
            ),

        combined = List.Combine({ collected, currentPage }),

        allPages =
            if hasNext and nextCursor <> null then
                @GetProducts(nextCursor, combined)
            else
                combined
    in
        allPages,

    // === Execute ===
    all = GetProducts(null, {}),

    // === Convert to Table ===
    result = Table.FromRecords(all),

    // === Types (keep all price fields as text!) ===
    typed =
        Table.TransformColumnTypes(
            result,
            {
                {"inventoryQuantity", Int64.Type},
                {"createdAt", type datetime},
                {"updatedAt", type datetime},
                {"publishedAt", type datetime},
                {"price", type text},
                {"compareAtPrice", type text},
                {"cost_amount", type text}
            }
        ),
    #"Filtered Rows" = Table.SelectRows(typed, each Date.IsInCurrentYear([publishedAt]))
in
    #"Filtered Rows"
