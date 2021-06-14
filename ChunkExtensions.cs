using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Lab.CSharp.Chunk
{
    public static class ChunkExtensions
    {
        // Get the IEnumerable<TValue> data structure associated to a given MethodCallExpression.
        private static IEnumerable<TValue> GetMethodCallExpressionEnumerable<TValue>(MethodCallExpression methodCall)
        {
            Expression parent = methodCall.Object ?? methodCall.Arguments.FirstOrDefault();
            return Expression.Lambda<Func<IEnumerable<TValue>>>(parent).Compile()();
        }

        /// <summary>
        /// Split the execution of a query containing .Contains(arg1, arg2, ..., argn) into chunks
        /// </summary>
        /// <typeparam name="T">The returned type</typeparam>
        /// <typeparam name="TValue">The type of argument evalued by the .Contains() call.</typeparam>
        /// <param name="query">The main query.</param>
        /// <param name="chunkSize">The expected size for the chunks.</param>
        /// <returns>A List of <T> items.</T></returns>
        public static List<T> ToChunkedExecutionList<T, TValue>(this IQueryable<T> query, int chunkSize)
        {
            Expression expression = query.Expression;

            var finder = new FindExpressionVisitor<MethodCallExpression>();

            finder.Visit(expression);

            MethodCallExpression containsCallExpression = finder.FoundExpressions.SingleOrDefault(p => p.Method.Name == "Contains");

            if (containsCallExpression == null)
            {
                throw new InvalidOperationException("A Contains() method is required but none was found in the expression.");
            }

            IEnumerable<TValue> localList = GetMethodCallExpressionEnumerable<TValue>(containsCallExpression);

            var result = new List<T>();
            var chunkList = new List<TValue>();

            MethodInfo containsMethod = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                                                          .Single(p => p.Name == "Contains" && p.GetParameters().Count() == 2)
                                                          .MakeGenericMethod(typeof(TValue));

            var parameter = new List<Expression>();
            parameter.Add(Expression.Constant(chunkList));

            if (containsCallExpression.Object == null)
            {
                parameter.AddRange(containsCallExpression.Arguments.Skip(1));
            }
            else
            {
                parameter.AddRange(containsCallExpression.Arguments);
            }

            var call = Expression.Call(containsMethod, parameter.ToArray());

            var replacer = new ReplaceExpressionVisitor(containsCallExpression, call);

            expression = replacer.Visit(expression);

            var chunkQuery = query.Provider.CreateQuery<T>(expression);

            for (int i = 0; i < Math.Ceiling((decimal)localList.Count() / chunkSize); i++)
            {
                chunkList.Clear();
                chunkList.AddRange(localList.Skip(i * chunkSize).Take(chunkSize));

                result.AddRange(chunkQuery.ToList());
            }

            return result;
        }

        /* Traverse an expression and search for subexpression having type T
         * All the subexpressions found will be added to the FoundExpressions list
         * Returns the original expression
         */
        private class FindExpressionVisitor<T> : ExpressionVisitor
            where T : Expression
        {
            public FindExpressionVisitor()
            {
                this.FoundExpressions = new List<T>();
            }

            public List<T> FoundExpressions { get; private set; }

            public override Expression Visit(Expression node)
            {
                var found = node as T;

                if (found != null)
                {
                    this.FoundExpressions.Add(found);
                }

                return base.Visit(node);
            }
        }

        /* Traverse an expression and try to replace a subexpression by an other
         * Replace a [source] subexpression node, if found, by a [target] subexpression node
         * Returns a new modified expression, or the original expression if [source] wasnt found
         */
        private class ReplaceExpressionVisitor : ExpressionVisitor
        {
            private Expression source;
            private Expression target;

            public ReplaceExpressionVisitor(Expression source, Expression target)
            {
                this.source = source;
                this.target = target;
            }

            public override Expression Visit(Expression node) => node == this.source ? this.target : base.Visit(node);
        }
    }
}