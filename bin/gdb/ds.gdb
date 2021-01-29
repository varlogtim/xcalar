# Quick and dirty GDB macro to dump all Datasets and it's user ref.
# XXX Todo parameterize this and use Python instead?
def iter
    set $ii = 0
    print Dataset::instance->datasetIdHashTable_
    while $ii < Dataset::NumDsHashSlots
	print $ii
	set $dataset = (DsDataset *) (&Dataset::instance->datasetIdHashTable_.slots)[0][$ii]
	while $dataset != 0
	    print *$dataset
	    set $jj = 0
	    while $jj < DsDataset::NumReferenceUserSlots
            	set $refcount = ('DsDataset::DsReferenceUserEntry' *) ((&($dataset->referenceUserTable_.slots))[0][$jj])
		while $refcount != 0
		    set $refcount = ('DsDataset::DsReferenceUserEntry' *) ((uintptr_t) $refcount - 0x110)
		    print *$refcount
		    set $refcount = ('DsDataset::DsReferenceUserEntry' *) $refcount->hook.next
		end
		set $jj = $jj +1
	    end
	    set $dataset = (DsDataset *) $dataset->intHook_.next
	end
        set $ii = $ii + 1
    end
end

document
    Print all the Datasets and it's users, references.
end
