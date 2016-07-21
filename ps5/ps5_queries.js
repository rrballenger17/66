/* 
 * CSCI E-66: Problem Set 5, MongoDB Programming Problems
 *
 * Put your name and email address below:
 *     name: Ryan Ballenger
 *     email: rrb@alumni.stanford.edu
 */

/*********************************************************************
 * REMEMBER:
 *  1. For each problem, you should assign your MongoDB method call 
 *     to the variable called "results" that we have provided. 
 *     Follow the model shown in the sample query below.
 *  2. You should *not* make any other additions or modifications to
 *     this file.
 *  3. You should test that the queries in this file are correct by
 *     executing all of the queries in the file from the command line.
 *     See the assignment for more details.
 *********************************************************************/

/* Do not modify the following lines. */
db = db.getSiblingDB('imdb')
function printResults(results) {
    if (results instanceof DBQuery) {
        results.forEach(printjson)
    } else if (Array.isArray(results)) {
	printjson(results)
    } else if (!isNaN(results)) {
        print(results)
    } else {
        printjson(results.result)
    }
}

/*
 * Sample query: Find the names of all movies in the database from 1990.
 */

print()
print("results of sample query")
print("-----------------------")

results = db.movies.find( { year: 1990 }, 
                          { name: 1, _id: 0 } )

printResults(results)


/*
 * Problem 5. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 5")
print("---------------------")

results = db.people.find({name: { $in: ["Leonardo DiCaprio", "Matt Damon"]}},{name:1,pob:1, dob:1, _id:0})

printResults(results)


/*
 * Problem 6. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 6")
print("---------------------")

results = db.oscars.find({"person.name": "Cate Blanchett"}, {_id:0,"movie.name":1,type:1,year:1 })

printResults(results)


/*
 * Problem 7. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 7")
print("---------------------")

results = db.people.find({hasDirected: true, hasActed:true, pob:/USA/},{name:1,pob:1})

printResults(results)


/*
 * Problem 8. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 8")
print("---------------------")

results = db.movies.find({$and: [{"actors.name": "Jennifer Lawrence"},{"actors.name": "Bradley Cooper"}]}, {name:1, _id:0, year:1})


printResults(results)


/*
 * Problem 9. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 9")
print("---------------------")

results = db.movies.count({$and: [{rating:"G"},{runtime: { $gt: 200 }}]}, {name:1, _id:0, runtime:1, rating:1})


printResults(results)


/*
 * Problem 10. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 10")
print("----------------------")

results = db.movies.aggregate({ $match: {runtime: { $gt: 200 }} },{$group: { _id: "$rating", num_long_movies: { $sum: 1 } }})

printResults(results)


/*
 * Problem 11. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 11")
print("----------------------")

results = db.movies.aggregate({ $match: {runtime: { $gt: 200 }} },{$group: { _id: "$rating", num_long_movies: { $sum: 1 } }},{$project: {rating:"$_id", num_long_movies:"$num_long_movies", _id:0}})

printResults(results)


/*
 * Problem 12. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 12")
print("----------------------")

results = db.oscars.distinct("movie.name", {$or: [{type:"BEST-PICTURE"},{type: "BEST-DIRECTOR"}],$and:[{year:{$gte: 2005 }},{year:{ $lte: 2015 }}]} )


printResults(results)


/*
 * Problem 13. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 13")
print("----------------------")

results = db.movies.aggregate({ $match: {earnings_rank: { $lte: 200 }} },{$group: { _id: "$directors.name", num_movies: { $sum: 1 } }},{ $match: {num_movies: { $gte: 4 }} },{$project: {director:"$_id", num_top_movies:"$num_movies", _id:0}},{ $unwind: "$director" })

printResults(results)


/*
 * Problem 14. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 */

print()
print("results for problem 14")
print("----------------------")

results = db.movies.aggregate({ $match: {rating: "PG"}},{$sort: { year: 1}},{$limit : 1 },{$project: {name:"$name", year:"$year", _id:0}})

printResults(results)


/*
 * Problem 15. Put your method call for this problem below,
 * assigning it to the results variable that we have provided.
 *
 * required for grad-credit students; optional for others
 */

print()
print("results for problem 15")
print("----------------------")

results = db.oscars.aggregate({ $match: {type: /ACT/} },{$group: { _id: {"name": "$person.name", "type": "$type"} }},{$group: { _id: "$_id.name", num_type: { $sum: 1 }}},{$match: {num_type:{ $gt: 1 }}},{$group: { _id: 0, count: { $sum: 1 }}},{$project: {_id: 0, count: "$count"}})

printResults(results)
