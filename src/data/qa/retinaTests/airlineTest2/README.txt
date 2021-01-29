This test is a witness to Xc-4362

Picture of the queryGraph is the png image in this folder.

If you study this queryGraph you'll notice that ArrDelay_integer is required
right at the end however there is a join along the way and it turns out
ArrDelay_integer came from the bottom table as a result of int(ArrDelay)
however, there is no guarantee that ArrDelay_integer cannot come from the
top table and so the query optimizer knows that and attempts to extract
ArrDelay_integer from the top dataset (the airports dataset) as well. The
problem is, CSV fields are all treated as strings so ArrDelay_integer coming
from the top is a string, but ArrDelay_integer coming from the bottom is an
integer and so where we do the final GroupBy step involving ArrDelay_integer
if for every single row, we extract the value of the integer variant of
ArrDelay_integer, we're good. If we ever extract the value of the string variant
of the ArrDelay_integer, we're screwed. The workaround is to inform the queryOptimizer
that user-defined fields are unique (this is not quite true). And so if the
queryOptimizer sees that the bottom table generated ArrDelay_integer, to not bother
to extract ArrDelay_integer from the top
