let
    Query1 = let
    // Shopify API credentials
    ShopifyApiUrl = "",
    AccessToken = "",

    // Constants
    MaxOrdersPerRequest = 250,
    StartDate = "2025-01-01T00:00:00Z",

    // GraphQL query with discount code, value, and customer email
    GraphQLQuery = "
      query ($cursor: String) {
        orders(first: 250, after: $cursor, query: ""created_at:>=" & StartDate & """) {
          edges {
            node {
              id
              name
              createdAt
              cancelledAt
              tags
              totalPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
              customer {
                email
              }
              discountApplications(first: 10) {
                edges {
                  node {
                    ... on DiscountCodeApplication {
                      code
                      value {
                        ... on MoneyV2 {
                          amount
                          currencyCode
                        }
                        ... on PricingPercentageValue {
                          percentage
                        }
                      }
                    }
                  }
                }
              }
              lineItems(first: 100) {
                edges {
                  node {
                    sku
                    title
                    quantity
                    originalUnitPriceSet {
                      shopMoney {
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
      }
    ",

    // Function to fetch orders with optional cursor
    FetchOrders = (cursor as nullable text) as record =>
        let
            bodyRecord = if cursor = null then
                [
                    query = GraphQLQuery,
                    variables = [ cursor = null ]
                ]
            else
                [
                    query = GraphQLQuery,
                    variables = [ cursor = cursor ]
                ],

            headers = [
                #"Content-Type" = "application/json",
                #"X-Shopify-Access-Token" = AccessToken
            ],

            response = Web.Contents(
                ShopifyApiUrl,
                [
                    Headers = headers,
                    Content = Json.FromValue(bodyRecord)
                ]
            ),
            jsonResponse = Json.Document(response)
        in
            jsonResponse,

    // Pagination loop to get all pages
    FetchAllOrders = () =>
        let
            Pages = List.Generate(
                () => [
                    cursor = null,
                    hasNext = true,
                    results = {}
                ],
                each [hasNext],
                each
                    let
                        result = FetchOrders([cursor]),
                        orders = result[data][orders][edges],
                        pageInfo = result[data][orders][pageInfo],
                        newCursor = pageInfo[endCursor],
                        hasNextPage = pageInfo[hasNextPage]
                    in
                        [
                            cursor = newCursor,
                            hasNext = hasNextPage,
                            results = orders
                        ],
                each [results]
            ),
            AllOrders = List.Combine(Pages),

            // Convert list of records to table
            OrdersTable = Table.FromList(AllOrders, Splitter.SplitByNothing(), null, null, ExtraValues.Error),

            // Expand the node record
            ExpandedNodes = Table.ExpandRecordColumn(OrdersTable, "Column1", {"node"}, {"node"}),

            // Expand main order fields (now includes customer)
            ExpandedOrders = Table.ExpandRecordColumn(ExpandedNodes, "node", {
                "id", "name", "createdAt", "cancelledAt", "tags", "totalPriceSet", 
                "discountApplications", "lineItems", "customer"
            }, {
                "id", "name", "createdAt", "cancelledAt", "tags", "totalPriceSet", 
                "discountApplications", "lineItems", "customer"
            }),

            // Expand customer record -> email
            ExpandedCustomer = Table.ExpandRecordColumn(ExpandedOrders, "customer", {"email"}, {"customerEmail"}),

            // Expand totalPriceSet > shopMoney
            ExpandedPriceSet = Table.ExpandRecordColumn(ExpandedCustomer, "totalPriceSet", {"shopMoney"}, {"shopMoney"}),
            ExpandedShopMoney = Table.ExpandRecordColumn(ExpandedPriceSet, "shopMoney", {"amount", "currencyCode"}, {"totalAmount", "totalCurrencyCode"}),

            // Expand discountApplications (record) to get edges (list)
            DiscountApplicationsEdges = Table.TransformColumns(ExpandedShopMoney, {
                {"discountApplications", each if _ <> null and Record.HasFields(_, "edges") then _[edges] else null, type list}
            }),
            DiscountListExpanded = Table.ExpandListColumn(DiscountApplicationsEdges, "discountApplications"),

            // Expand discount node details
            DiscountDetails = Table.ExpandRecordColumn(DiscountListExpanded, "discountApplications", {"node"}, {"discountNode"}),
            DiscountNodeExpanded = Table.ExpandRecordColumn(DiscountDetails, "discountNode", {"code", "value"}, {"discountCode", "discountValue"}),

            // Expand discountValue which is a record with either MoneyV2 or PricingPercentageValue
            DiscountValueExpanded = Table.ExpandRecordColumn(DiscountNodeExpanded, "discountValue", {"amount", "currencyCode", "percentage"}, {"discountAmount", "discountCurrencyCode", "discountPercentage"}),

            // Expand lineItems (record) to edges (list)
            LineItemsEdges = Table.TransformColumns(DiscountValueExpanded, {
                {"lineItems", each if _ <> null and Record.HasFields(_, "edges") then _[edges] else null, type list}
            }),
            LineItemsExpanded = Table.ExpandListColumn(LineItemsEdges, "lineItems"),

            // Expand lineItem node details
            LineItemDetails = Table.ExpandRecordColumn(LineItemsExpanded, "lineItems", {"node"}, {"lineItemNode"}),
            LineItemFields = Table.ExpandRecordColumn(LineItemDetails, "lineItemNode", {"sku", "title", "quantity", "originalUnitPriceSet"}, {"sku", "title", "quantity", "originalUnitPriceSet"}),
            LineItemPriceExpanded = Table.ExpandRecordColumn(LineItemFields, "originalUnitPriceSet", {"shopMoney"}, {"lineItemShopMoney"}),
            LineItemPriceDetails = Table.ExpandRecordColumn(LineItemPriceExpanded, "lineItemShopMoney", {"amount", "currencyCode"}, {"lineItemAmount", "lineItemCurrencyCode"})
        in
            LineItemPriceDetails
in
    FetchAllOrders(),
    #"Extracted Values" = Table.TransformColumns(Query1, {"tags", each Text.Combine(List.Transform(_, Text.From)), type text}),
    #"Added Custom" = Table.AddColumn(#"Extracted Values", "Total Quantity", each let
        sku = Text.From([sku]),
        qty = try Number.From([quantity]) otherwise 1,
        dashPos = Text.PositionOf(sku, "-", Occurrence.Last),
        suffix = if dashPos <> -1 then Text.Middle(sku, dashPos + 1) else "",
        isNumeric = Text.Select(suffix, {"0".."9"}) = suffix and Text.Length(suffix) > 0,
        hiddenQty = if isNumeric then Number.From(suffix) else 1
    in
        qty * hiddenQty),
    #"Replaced Value" = Table.ReplaceValue(#"Added Custom",".",",",Replacer.ReplaceText,{"totalAmount"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Replaced Value",{{"totalAmount", Currency.Type}}),
    #"Replaced Value1" = Table.ReplaceValue(#"Changed Type",".",",",Replacer.ReplaceText,{"lineItemAmount"}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Replaced Value1",{
        {"lineItemAmount", Currency.Type}, 
        {"quantity", Int64.Type}, 
        {"discountPercentage", Int64.Type}, 
        {"createdAt", type datetime}, 
        {"Total Quantity", Int64.Type}
    }),
    #"Merged Queries" = Table.NestedJoin(#"Changed Type1", {"sku"}, Products, {"sku"}, "Products", JoinKind.LeftOuter),
    #"Expanded Products" = Table.ExpandTableColumn(#"Merged Queries", "Products", {"product_title", "product_type", "createdAt", "publishedAt", "variant_title", "price", "cost_amount", "inventoryQuantity"}, {"Products.product_title", "Products.product_type", "Products.createdAt", "Products.publishedAt", "Products.variant_title", "Products.price", "Products.cost_amount", "Products.inventoryQuantity"}),
    #"Filtered Rows" = Table.SelectRows(#"Expanded Products", each ([Products.product_title] <> null)),
    #"Changed Type2" = Table.TransformColumnTypes(#"Filtered Rows",{{"Products.price", Currency.Type}, {"Products.cost_amount", Currency.Type}})
in
    #"Changed Type2"
