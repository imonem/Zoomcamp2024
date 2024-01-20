# Module 1 Homework

## Docker & SQL

### Question 1. Knowing docker tags

- `--rm` has the *Automatically remove the container when it exits*

### Question 2. Understanding docker first run

What is version of the package wheel ?

- 0.42.0

### Question 3. Count records

How many taxi trips were totally made on September 18th 2019?

- 15612

### Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance Use the pick up time for your calculations.

- 2019-09-26

### Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

- "Brooklyn" "Manhattan" "Queens"

### Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's ```tip``` , not ```trip```

- JFK Airport

## Terraform

### Question 7. Creating Resources

```bash
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demodataset2024 will be created
  + resource "google_bigquery_dataset" "demodataset2024" {
      + creation_time              = (known after apply)
      + dataset_id                 = "demodataset2024id"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "EU"
      + max_time_travel_hours      = (known after apply)
      + project                    = "zoomcamp2024-411708"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

  # google_storage_bucket.demobucket2024 will be created
  + resource "google_storage_bucket" "demobucket2024" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EU"
      + name                        = "zoomcamp2024-411708-bucket"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + rpo                         = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "AbortIncompleteMultipartUpload"
            }
          + condition {
              + age                   = 1
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demodataset2024: Creating...
google_storage_bucket.demobucket2024: Creating...
google_storage_bucket.demobucket2024: Creation complete after 2s [id=zoomcamp2024-411708-bucket]
google_bigquery_dataset.demodataset2024: Creation complete after 2s [id=projects/zoomcamp2024-411708/datasets/demodataset2024id]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```