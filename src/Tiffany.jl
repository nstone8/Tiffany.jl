module Tiffany
using DataFrames, FileIO, CSVFiles

export abundancenorm

function abundancenorm(df::DataFrame)
    #make a copy so we don't break the frame we're passed
    frame = copy(df)
    #drop some columns
    select!(frame,Not(r".*Combined.*"))
    select!(frame,Not(r".*Unique Spectral.*"))
    select!(frame,Not(r".*Total Spectral.*"))
    select!(frame,Not(r".*Intensity.*"))
    #get the spectral count column for each sample and capture the sample name
    sampcolmatches = match.(r"(.*) Spectral Count$",names(frame))
    filter!(sampcolmatches) do scm
        !isnothing(scm)
    end
    select!(frame,vcat("Entry Name","Gene",[scm.match for scm in sampcolmatches]))
    transformers = map(sampcolmatches) do scm
        @assert length(scm.captures) == 1 "should only be one sample name per spectral count column"
        scm.match => ((col) -> col / sum(col)) => "$(scm.captures[1])_norm"
    end
    transform!(frame,transformers...)
    return frame
end

function abundancenorm(path::String)
    load(path) |> DataFrame |> abundancenorm
end

end # module Tiffany
