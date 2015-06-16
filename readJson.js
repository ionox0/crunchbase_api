var fs = require('fs');
var obj = JSON.parse(fs.readFileSync('categories.json', 'utf8'));
var total = 0;

// Count total organizations
obj.items.forEach(function(item){ total += item.properties.organizations_in_category })
console.log(total);

// Write categories info to file
obj.items.forEach(function(item){ 
	fs.appendFile('allCategories', item.uuid + ', ' + item.properties.name + ', ' + item.properties.organizations_in_category +  '\n');
})