package main

import "errors"

// A source cursor may advance only after every accepted job, including the
// active final job, has reported durable completion. Drain all results even
// after a failure so a retry cannot overlap the previous batch.
func waitTrackedIngestJobs(results []<-chan error) error {
	var failure error
	for _, result := range results {
		if err := <-result; err != nil {
			failure = errors.Join(failure, err)
		}
	}
	return failure
}

// Both historical sources use the same durability barrier. Saving the cursor
// is part of completion, and is never attempted after a write/admission error.
func finishSourceBatch(results []<-chan error, admissionFailed bool, save func() error) error {
	err := waitTrackedIngestJobs(results)
	if admissionFailed {
		err = errors.Join(err, errors.New("historical source batch admission failed"))
	}
	if err != nil {
		return err
	}
	return save()
}
