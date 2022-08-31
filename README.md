# leaftrade-epicor
Data Pipeline Integration of Leaf Trade software with wholesale Epicor ERP

Web app constructed to reduce duplicated entry/ user error in orders placed in LeafTrade when translated to Epicor ERP system. <br />
Users will visit site after marking orders as approved in leaftrade to initiate the migration to ERP. They are allowed to pull in all orders marked as approved or individual orders by order_id from LeafTrade.<br />
<br />
<h2>Feature Updates</h2>
* Additional functionality added was an attribute editor to help alter product/batch variant data which felt cumbersome to do en-masse with utilizing the user interface in the current LeafTrade platform.<br />
* Updated to provide same work flow as orders to migrate Product Issues (see: return material authorizations).<br />
* Ability to pull down current available items on storefront to know what was sold on that day.<br />
* Functionality to check order allocations against inventory to prevent overselling or product mismatch between ERP and B2B storefront.<br />
* Updated to utilize multithreading in order to speed up the transaction time based on several I/O events.<br />
* Abstracted out private keys to environmental variables to ensure better security practices.<br />
<br />
<h3>Tech Stack</h3>
Flask App built with pandas, hosted on internal IIS. Epicor 10.2 client with REST services enabled.